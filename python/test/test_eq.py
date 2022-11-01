import unittest
import os
import json
import logging

from eqsql import eq

# Assumes the existence of a testing database
# with these characteristics
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_USER'] = 'eqsql_test_user'
os.environ['DB_PORT'] = '5433'
os.environ['DB_NAME'] = 'eqsql_test_db'


def create_payload(x=1.2):
    payload = {'x': x, 'y': 7.3, 'z': 'foo'}
    return json.dumps(payload)


clear_db_sql = """
delete from eq_exp_id_tasks;
delete from eq_tasks;
delete from emews_queue_OUT;
delete from emews_queue_IN;
delete from eq_task_tags;
alter sequence emews_id_generator restart;
"""


def clear_db(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(clear_db_sql)


class EQTests(unittest.TestCase):

    def tearDown(self):
        eq.close()

    def test_submit(self):
        self.assertIsNone(eq._DB)
        # test before init, so fails
        # turn off the exception printing of errors, so we get
        # clean test results
        eq.logger.setLevel(logging.CRITICAL)
        result_status, ft = eq.submit_task('test_future', 0, create_payload(), tag='x')
        self.assertEqual(eq.ResultStatus.FAILURE, result_status)
        self.assertIsNone(ft)
        eq.logger.setLevel(logging.WARN)

        eq.init()
        clear_db(eq._DB.conn)
        result_status, ft = eq.submit_task('test_future', 0, create_payload(), tag='x')
        self.assertEqual(eq.ResultStatus.SUCCESS, result_status)
        self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)
        eq.close()

    def test_query_priority(self):
        eq.init()
        clear_db(eq._DB.conn)
        result_status, ft = eq.submit_task('test_future', 0, create_payload(), priority=10, tag='x')
        self.assertEqual(eq.ResultStatus.SUCCESS, result_status)
        self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)
        self.assertEqual(10, ft.priority)

        ft.priority = 20
        self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)
        self.assertEqual(20, ft.priority)

        eq.close()

    def test_query_result(self):
        eq.init()
        clear_db(eq._DB.conn)

        # no task so query timesout
        result = eq.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(eq.EQ_TIMEOUT, result['payload'])

        payload = create_payload()
        _, ft = eq.submit_task('test_future', 0, payload)
        self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(eq.ResultStatus.FAILURE, result_status)
        self.assertEqual(result, eq.EQ_TIMEOUT)

        result = eq.query_task(0, timeout=0.5)
        self.assertEqual('work', result['type'])
        task_id = result['eq_task_id']
        self.assertEqual(ft.eq_task_id, task_id)
        self.assertEqual(payload, result['payload'])

        # test result still failure, and status is running
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(eq.ResultStatus.FAILURE, result_status)
        self.assertEqual(result, eq.EQ_TIMEOUT)
        task_status = ft.status
        self.assertEqual(eq.TaskStatus.RUNNING, task_status)
        self.assertFalse(ft.done())

        # report task result
        task_result = {'j': 3}
        report_result = eq.report_task(task_id, 0, json.dumps(task_result))
        self.assertEqual(eq.ResultStatus.SUCCESS, report_result)

        # test get result
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(eq.ResultStatus.SUCCESS, result_status)
        self.assertEqual(task_result, json.loads(result))

        # test status
        task_status = ft.status
        self.assertEqual(eq.TaskStatus.COMPLETE, task_status)
        self.assertTrue(ft.done())

        # test eq stop
        eq.stop_worker_pool(0)
        result = eq.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(eq.EQ_STOP, result['payload'])

        eq.close()
        self.assertIsNone(eq._DB)

    def test_query_more(self):
        eq.init()
        # eq.logger.setLevel(logging.DEBUG)
        clear_db(eq._DB.conn)

        fts = {}
        payloads = {}
        # ME: submit tasks
        for i in range(0, 8):
            payload = create_payload(i)
            _, ft = eq.submit_task('test_future', 0, payload)
            fts[ft.eq_task_id] = ft
            payloads[ft.eq_task_id] = payload
            self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())

        # Worker Pool: get 4 tasks
        batch_size = 4
        # currently running_task_ids and new tasks
        running_task_ids, tasks = eq.query_more_tasks(0, eq_task_ids=[],
                                                      batch_size=batch_size)

        # nothing running prior to the above call
        self.assertEqual([1, 2, 3, 4], running_task_ids)
        self.assertEqual(4, len(tasks))

        # WP: complete 2 tasks
        for task in tasks[:2]:
            task_id = task['eq_task_id']
            task_result = {'j': task_id}
            eq.report_task(task_id, 0, json.dumps(task_result))

        # WP: Completed 2 tasks of the current tasks, so 2 new "slots" available
        running_task_ids, tasks = eq.query_more_tasks(0, eq_task_ids=running_task_ids,
                                                      batch_size=batch_size)
        self.assertEqual(4, len(running_task_ids))
        # running_task_ids now contains the original 2 (3 and 4) that had not yet
        # finished, and the two new tasks: 5 and 6.
        self.assertEqual([3, 4, 5, 6], running_task_ids)
        self.assertEqual(2, len(tasks))

        # complete all the running tasks. So, 2 left in queue
        for task_id in running_task_ids:
            task_result = {'j': task_id}
            eq.report_task(task_id, 0, json.dumps(task_result))

        # WP: ask for 4, should get remaining 2
        running_task_ids, tasks = eq.query_more_tasks(0, eq_task_ids=running_task_ids,
                                                      batch_size=batch_size)
        self.assertEqual(2, len(running_task_ids))
        self.assertEqual([7, 8], running_task_ids)
        self.assertEqual(2, len(tasks))

        eq.close()

    def test_query_task_n(self):
        eq.init()
        clear_db(eq._DB.conn)

        # no task so query timesout
        result = eq.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(eq.EQ_TIMEOUT, result['payload'])

        fts = {}
        payloads = {}
        for i in range(0, 8):
            payload = create_payload(i)
            _, ft = eq.submit_task('test_future', 0, payload)
            fts[ft.eq_task_id] = ft
            payloads[ft.eq_task_id] = payload
            self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())

        results = eq.query_task(0, n=4, timeout=0.5)
        self.assertEqual(4, len(results))
        for result in results:
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            self.assertEqual(payloads[task_id], result['payload'])
            ft = fts[task_id]
            # test result still failure, and status is running
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(eq.ResultStatus.FAILURE, result_status)
            self.assertEqual(result, eq.EQ_TIMEOUT)
            task_status = ft.status
            self.assertEqual(eq.TaskStatus.RUNNING, task_status)
            self.assertFalse(ft.done())

        results = eq.query_task(0, n=2, timeout=0.5)
        self.assertEqual(2, len(results))
        for result in results:
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            self.assertEqual(payloads[task_id], result['payload'])
            ft = fts[task_id]
            # test result still failure, and status is running
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(eq.ResultStatus.FAILURE, result_status)
            self.assertEqual(result, eq.EQ_TIMEOUT)
            task_status = ft.status
            self.assertEqual(eq.TaskStatus.RUNNING, task_status)
            self.assertFalse(ft.done())

        eq.stop_worker_pool(0)
        results = eq.query_task(0, n=10, timeout=0.5)
        # 3 - 2 remaining tasks and stop
        self.assertEqual(3, len(results))
        for result in results[:-1]:
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            self.assertEqual(payloads[task_id], result['payload'])
            ft = fts[task_id]
            # test result still failure, and status is running
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(eq.ResultStatus.FAILURE, result_status)
            self.assertEqual(result, eq.EQ_TIMEOUT)
            task_status = ft.status
            self.assertEqual(eq.TaskStatus.RUNNING, task_status)
            self.assertFalse(ft.done())

        result = results[-1]
        self.assertEqual('status', result['type'])
        self.assertEqual(eq.EQ_STOP, result['payload'])

        eq.close()
        self.assertIsNone(eq._DB)

    def test_priority(self):
        eq.init()
        clear_db(eq._DB.conn)

        # test priority, add mult work with different priority
        # get in order
        fs = []
        for i in range(0, 4):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=i)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append((payload, ft))

        for i in range(3, -1, -1):
            result = eq.query_task(0, timeout=0.5)
            self.assertEqual('work', result['type'])
            payload, ft = fs[i]
            task_id = result['eq_task_id']
            self.assertEqual(ft.eq_task_id, task_id)
            self.assertEqual(payload, result['payload'])

        eq.close()

    def test_work_type(self):
        # add different work types and get by type
        eq.init()
        clear_db(eq._DB.conn)

        # test priority, add mult work with different priority
        # get in order
        fs = []
        for i in range(0, 4):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', i, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append((payload, ft))

        for i in [1, 0, 3, 2]:
            result = eq.query_task(i, timeout=0.5)
            self.assertEqual('work', result['type'])
            payload, ft = fs[i]
            task_id = result['eq_task_id']
            self.assertEqual(ft.eq_task_id, task_id)
            self.assertEqual(payload, result['payload'])

        eq.close()

    def test_no_work(self):
        eq.init()
        clear_db(eq._DB.conn)
        # query for work when no work

        result = eq.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(eq.EQ_TIMEOUT, result['payload'])

        eq.close()

    def test_cancel(self):
        eq.init()
        clear_db(eq._DB.conn)
        result_status, ft = eq.submit_task('test_future', 0, create_payload(), tag='x')
        self.assertEqual(eq.ResultStatus.SUCCESS, result_status)
        self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())

        result = ft.cancel()
        self.assertTrue(result)
        self.assertEqual(eq.TaskStatus.CANCELED, ft.status)
        self.assertTrue(ft.done())

        # no work because canceled
        result = eq.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(eq.EQ_TIMEOUT, result['payload'])

        result = ft.cancel()
        self.assertTrue(result)

        eq.close()

    def test_as_completed(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        timedout = False
        try:
            for ft in eq.as_completed(fs, timeout=5):
                pass
            self.fail('timeout exception expected')
        except eq.TimeoutError:
            timedout = True
        self.assertTrue(timedout)

        count = 0
        while True:
            result = eq.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            count += 1
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = eq.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(eq.ResultStatus.SUCCESS, report_result)

        self.assertEqual(200, count)

        for ft in eq.as_completed(fs, timeout=None):
            self.assertTrue(ft.done())
            self.assertEqual(eq.TaskStatus.COMPLETE, ft.status)
            status, result_str = ft.result(timeout=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, status)
            self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])

        eq.close()

    def test_as_completed_stop(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 10 results
        for _ in range(10):
            result = eq.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = eq.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(eq.ResultStatus.SUCCESS, report_result)

        excepted = False
        count = 0
        try:
            for ft in eq.as_completed(fs, timeout=None, stop_condition=lambda: True):
                count += 1
                self.assertTrue(ft.done())
                self.assertEqual(eq.TaskStatus.COMPLETE, ft.status)
                status, result_str = ft.result(timeout=0)
                self.assertEqual(eq.ResultStatus.SUCCESS, status)
                self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])
            self.fail('exception not thrown')
        except eq.StopConditionException:
            excepted = True

        self.assertTrue(excepted)
        self.assertEqual(10, count)

        eq.close()

    def test_as_completed_n(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 100 results
        for _ in range(100):
            result = eq.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = eq.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(eq.ResultStatus.SUCCESS, report_result)

        count = 0
        for ft in eq.as_completed(fs, timeout=None, n=10):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(eq.TaskStatus.COMPLETE, ft.status)
            status, result_str = ft.result(timeout=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, status)
            self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])

        self.assertEqual(10, count)
        eq.close()

    def test_as_completed_abort(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        for i in range(10):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        eq.close()

        # turn off the exception printing of errors, so we get
        # clean test results
        eq.logger.setLevel(logging.CRITICAL)
        count = 0
        # this will produce exception output through logger.error calls
        # because eq.DB is now None
        for ft in eq.as_completed(fs, timeout=None):
            count += 1
            self.assertFalse(ft.done())
            status, result_str = ft.result(timeout=0)
            self.assertEqual(eq.ResultStatus.FAILURE, status)
            self.assertEqual(result_str, eq.EQ_ABORT)

        self.assertEqual(10, count)
        eq.logger.setLevel(logging.WARN)

    def test_as_completed_pop(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        # 100 submissions
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 100 results
        for _ in range(100):
            result = eq.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = eq.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(eq.ResultStatus.SUCCESS, report_result)

        fs_len = len(fs)
        ft = eq.pop_completed(fs)
        self.assertTrue(ft.done())
        self.assertEqual(eq.TaskStatus.COMPLETE, ft.status)
        self.assertEqual(fs_len - 1, len(fs))

        n = 10
        fs_len = len(fs)
        count = 0
        for ft in eq.as_completed(fs, pop=True, n=n):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(eq.TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len - count, len(fs))

        self.assertEqual(fs_len - n, len(fs))
        eq.close()

    def test_cancel_tasks(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        status, count = eq.cancel(fs)
        self.assertEqual(eq.ResultStatus.SUCCESS, status)
        self.assertEqual(200, count)

        for f in fs:
            self.assertEqual(eq.TaskStatus.CANCELED, f.status)

        eq.close()

    def test_update_priorities(self):
        eq.init()
        clear_db(eq._DB.conn)

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = eq.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(eq.ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        status, count = eq.update_priority(fs, 10)
        self.assertEqual(eq.ResultStatus.SUCCESS, status)
        self.assertEqual(200, count)

        for f in fs:
            self.assertEqual(10, f.priority)

        eq.close()
