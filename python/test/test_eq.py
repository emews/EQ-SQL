import unittest
import json
import logging
import os
import shutil

from eqsql.task_queues import local_queue
from eqsql.task_queues.core import ResultStatus, TaskStatus, TimeoutError
from eqsql.task_queues.core import EQ_TIMEOUT, EQ_STOP, EQ_ABORT
from eqsql.db_tools import reset_db, init_eqsql_db, start_db, stop_db, is_db_running
from eqsql.cfg import parse_yaml_cfg

# Assumes the existence of a testing database
# with these characteristics
host = 'localhost'
user = 'eqsql_test_user'
port = 5444
db_name = 'eqsql_test_db'
password = None

db_path = './test_data/db/eqsql_test'
pg_bin = '/home/nick/sfw/postgresql-14.11/bin'


def create_payload(x=1.2):
    payload = {'x': x, 'y': 7.3, 'z': 'foo'}
    return json.dumps(payload)


def clear_db():
    reset_db(user, db_name, host, port, password)


class EQTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        init_eqsql_db(db_path, db_user=user, db_name=db_name,
                      db_port=port, pg_bin_path=pg_bin)
        if not is_db_running(db_path, port, pg_bin):
            start_db(db_path, pg_bin, port)

    @classmethod
    def tearDownClass(cls):
        stop_db(db_path, pg_bin, port)

    def tearDown(self):
        self.eq_sql.close()

    def test_submit(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()
        result_status, ft = self.eq_sql.submit_task('test_future', 0, create_payload(), tag='x')
        self.assertEqual(ResultStatus.SUCCESS, result_status)
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)

    def test_query_priority(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()
        result_status, ft = self.eq_sql.submit_task('test_future', 0, create_payload(), priority=10, tag='x')
        self.assertEqual(ResultStatus.SUCCESS, result_status)
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)
        self.assertEqual(10, ft.priority)

        ft.priority = 20
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)
        self.assertEqual(20, ft.priority)

    def test_query_result(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        # no task so query timesout
        result = self.eq_sql.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(EQ_TIMEOUT, result['payload'])

        payload = create_payload()
        _, ft = self.eq_sql.submit_task('test_future', 0, payload)
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertIsNone(ft.worker_pool)
        self.assertFalse(ft.done())
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(ResultStatus.FAILURE, result_status)
        self.assertEqual(result, EQ_TIMEOUT)

        result = self.eq_sql.query_task(0, timeout=0.5)
        self.assertEqual('work', result['type'])
        task_id = result['eq_task_id']
        self.assertEqual(ft.eq_task_id, task_id)
        self.assertEqual(payload, result['payload'])

        # test result still failure, and status is running
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(ResultStatus.FAILURE, result_status)
        self.assertEqual(result, EQ_TIMEOUT)
        task_status = ft.status
        self.assertEqual(TaskStatus.RUNNING, task_status)
        self.assertEqual('default', ft.worker_pool)
        self.assertFalse(ft.done())

        # report task result
        task_result = {'j': 3}
        report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
        self.assertEqual(ResultStatus.SUCCESS, report_result)

        # test get result
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(ResultStatus.SUCCESS, result_status)
        self.assertEqual(task_result, json.loads(result))

        # test status
        task_status = ft.status
        self.assertEqual(TaskStatus.COMPLETE, task_status)
        self.assertTrue(ft.done())

        # test eq stop
        self.eq_sql.stop_worker_pool(0)
        result = self.eq_sql.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(EQ_STOP, result['payload'])

        self.eq_sql.close()
        self.assertIsNone(self.eq_sql.db)

    def test_get(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        # eq.logger.setLevel(logging.DEBUG)
        clear_db()

        fts = {}
        payloads = {}
        # ME: submit tasks
        for i in range(0, 8):
            payload = create_payload(i)
            _, ft = self.eq_sql.submit_task('test_future', 0, payload)
            fts[ft.eq_task_id] = ft
            payloads[ft.eq_task_id] = payload
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())

        query = "select eq_task_id, json_out from eq_tasks where eq_task_id in (%s, %s, %s)"
        status, result = self.eq_sql._get(query, 1, 2, 3)
        self.assertEqual(status, ResultStatus.SUCCESS)
        self.assertEqual(3, len(result))
        ids = [1, 2, 3]
        for eq_task_id, payload in result:
            self.assertTrue(eq_task_id in ids)
            ids.remove(eq_task_id)
            exp_payload = {"x": eq_task_id - 1, "y": 7.3, "z": "foo"}
            self.assertEqual(json.dumps(exp_payload), payload)

    def test_query_more(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        # eq.logger.setLevel(logging.DEBUG)
        clear_db()

        fts = {}
        payloads = {}
        # ME: submit tasks
        for i in range(0, 8):
            payload = create_payload(i)
            _, ft = self.eq_sql.submit_task('test_future', 0, payload)
            fts[ft.eq_task_id] = ft
            payloads[ft.eq_task_id] = payload
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())

        pools = self.eq_sql.get_worker_pools([ft for ft in fts.values()])
        self.assertEqual(8, len(pools))
        for _, p in pools:
            self.assertIsNone(p)

        # Worker Pool: get 4 tasks
        batch_size = 4
        # currently running_task_ids and new tasks
        running_task_ids, tasks = self.eq_sql.query_more_tasks(0, eq_task_ids=[],
                                                               batch_size=batch_size, worker_pool='P1')

        # nothing running prior to the above call
        self.assertEqual([1, 2, 3, 4], running_task_ids)
        self.assertEqual(4, len(tasks))

        pools = self.eq_sql.get_worker_pools([ft for ft in fts.values()])
        self.assertEqual(8, len(pools))
        p1s = 0
        for ft, pool in pools:
            if ft.eq_task_id in running_task_ids:
                self.assertEqual('P1', pool)
                p1s += 1
            else:
                self.assertIsNone(pool)
        self.assertEqual(p1s, len(running_task_ids))

        # WP: complete 2 tasks
        for task in tasks[:2]:
            task_id = task['eq_task_id']
            task_result = {'j': task_id}
            self.eq_sql.report_task(task_id, 0, json.dumps(task_result))

        # WP: Completed 2 tasks of the current tasks, so 2 new "slots" available
        running_task_ids, tasks = self.eq_sql.query_more_tasks(0, eq_task_ids=running_task_ids,
                                                               batch_size=batch_size)
        self.assertEqual(4, len(running_task_ids))
        # running_task_ids now contains the original 2 (3 and 4) that had not yet
        # finished, and the two new tasks: 5 and 6.
        self.assertEqual([3, 4, 5, 6], running_task_ids)
        self.assertEqual(2, len(tasks))

        # complete all the running tasks. So, 2 left in queue
        for task_id in running_task_ids:
            task_result = {'j': task_id}
            self.eq_sql.report_task(task_id, 0, json.dumps(task_result))

        # WP: ask for 4, should get remaining 2
        running_task_ids, tasks = self.eq_sql.query_more_tasks(0, eq_task_ids=running_task_ids,
                                                               batch_size=batch_size)
        self.assertEqual(2, len(running_task_ids))
        self.assertEqual([7, 8], running_task_ids)
        self.assertEqual(2, len(tasks))

    def test_query_task_n(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        # no task so query timesout
        result = self.eq_sql.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(EQ_TIMEOUT, result['payload'])

        fts = {}
        payloads = {}
        for i in range(8):
            payload = create_payload(i)
            _, ft = self.eq_sql.submit_task('test_future', 0, payload)
            fts[ft.eq_task_id] = ft
            payloads[ft.eq_task_id] = payload
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())

        results = self.eq_sql.query_task(0, n=4, worker_pool='P1', timeout=0.5)
        self.assertEqual(4, len(results))
        running_fts = []
        for result in results:
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            self.assertEqual(payloads[task_id], result['payload'])
            ft = fts[task_id]
            running_fts.append(ft)
            self.assertEqual('P1', ft.worker_pool)
            # test result still failure, and status is running
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(ResultStatus.FAILURE, result_status)
            self.assertEqual(result, EQ_TIMEOUT)
            task_status = ft.status
            self.assertEqual(TaskStatus.RUNNING, task_status)
            self.assertFalse(ft.done())

        pools = self.eq_sql.get_worker_pools(running_fts)
        for _, pool in pools:
            self.assertEqual('P1', pool)

        results = self.eq_sql.query_task(0, n=2, timeout=0.5)
        self.assertEqual(2, len(results))
        for result in results:
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            self.assertEqual(payloads[task_id], result['payload'])
            ft = fts[task_id]
            # test result still failure, and status is running
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(ResultStatus.FAILURE, result_status)
            self.assertEqual(result, EQ_TIMEOUT)
            task_status = ft.status
            self.assertEqual(TaskStatus.RUNNING, task_status)
            self.assertFalse(ft.done())

        self.eq_sql.stop_worker_pool(0)
        results = self.eq_sql.query_task(0, n=10, timeout=0.5)
        # 3 - 2 remaining tasks and stop
        self.assertEqual(3, len(results))
        for result in results[:-1]:
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            self.assertEqual(payloads[task_id], result['payload'])
            ft = fts[task_id]
            # test result still failure, and status is running
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(ResultStatus.FAILURE, result_status)
            self.assertEqual(result, EQ_TIMEOUT)
            task_status = ft.status
            self.assertEqual(TaskStatus.RUNNING, task_status)
            self.assertFalse(ft.done())

        result = results[-1]
        self.assertEqual('status', result['type'])
        self.assertEqual(EQ_STOP, result['payload'])

    def test_priority(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        # test priority, add mult work with different priority
        # get in order
        fs = []
        for i in range(0, 4):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=i)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append((payload, ft))

        for i in range(3, -1, -1):
            result = self.eq_sql.query_task(0, timeout=0.5)
            self.assertEqual('work', result['type'])
            payload, ft = fs[i]
            task_id = result['eq_task_id']
            self.assertEqual(ft.eq_task_id, task_id)
            self.assertEqual(payload, result['payload'])

    def test_work_type(self):
        # add different work types and get by type
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        # test priority, add mult work with different priority
        # get in order
        fs = []
        for i in range(0, 4):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', i, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append((payload, ft))

        for i in [1, 0, 3, 2]:
            result = self.eq_sql.query_task(i, timeout=0.5)
            self.assertEqual('work', result['type'])
            payload, ft = fs[i]
            task_id = result['eq_task_id']
            self.assertEqual(ft.eq_task_id, task_id)
            self.assertEqual(payload, result['payload'])

    def test_no_work(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()
        # query for work when no work

        result = self.eq_sql.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(EQ_TIMEOUT, result['payload'])

        self.eq_sql.close()

    def test_cancel(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()
        result_status, ft = self.eq_sql.submit_task('test_future', 0, create_payload(), tag='x')
        self.assertEqual(ResultStatus.SUCCESS, result_status)
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())

        result = ft.cancel()
        self.assertTrue(result)
        self.assertEqual(TaskStatus.CANCELED, ft.status)
        self.assertTrue(ft.done())

        # no work because canceled
        result = self.eq_sql.query_task(0, timeout=0.5)
        self.assertEqual('status', result['type'])
        self.assertEqual(EQ_TIMEOUT, result['payload'])

        result = ft.cancel()
        self.assertTrue(result)

    def test_as_completed(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        timedout = False
        try:
            for ft in self.eq_sql.as_completed(fs, timeout=5):
                pass
            self.fail('timeout exception expected')
        except TimeoutError:
            timedout = True
        self.assertTrue(timedout)

        count = 0
        while True:
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            count += 1
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        self.assertEqual(200, count)

        for ft in self.eq_sql.as_completed(fs, timeout=None):
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            status, result_str = ft.result(timeout=0)
            self.assertEqual(ResultStatus.SUCCESS, status)
            self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])

        self.eq_sql.close()

    def test_as_completed_stop(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 10 results
        for _ in range(10):
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        # excepted = False
        # count = 0
        # try:
        #     for ft in self.eq_sql.as_completed(fs, timeout=None, stop_condition=lambda: True):
        #         count += 1
        #         self.assertTrue(ft.done())
        #         self.assertEqual(TaskStatus.COMPLETE, ft.status)
        #         status, result_str = ft.result(timeout=0)
        #         self.assertEqual(ResultStatus.SUCCESS, status)
        #         self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])
        #     self.fail('exception not thrown')
        # except StopConditionException:
        #     excepted = True

        # self.assertTrue(excepted)
        # self.assertEqual(10, count)

        self.eq_sql.close()

    def test_as_completed_n(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 100 results
        for _ in range(100):
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        count = 0
        for ft in self.eq_sql.as_completed(fs, timeout=None, n=10):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            status, result_str = ft.result(timeout=0)
            self.assertEqual(ResultStatus.SUCCESS, status)
            self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])

        self.assertEqual(10, count)
        self.eq_sql.close()

    def test_as_completed_abort(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        for i in range(10):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        self.eq_sql.close()

        # turn off the exception printing of errors, so we get
        # clean test results
        self.eq_sql.logger.setLevel(logging.CRITICAL)
        count = 0
        # this will produce exception output through logger.error calls
        # because eq.DB is now None
        for ft in self.eq_sql.as_completed(fs, timeout=None):
            count += 1
            self.assertFalse(ft.done())
            status, result_str = ft.result(timeout=0)
            self.assertEqual(ResultStatus.FAILURE, status)
            self.assertEqual(result_str, EQ_ABORT)

        self.assertEqual(10, count)
        self.eq_sql.logger.setLevel(logging.WARN)

    def test_as_completed_pop(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        # 100 submissions
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 100 results
        for _ in range(100):
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        fs_len = len(fs)
        ft = self.eq_sql.pop_completed(fs)
        self.assertTrue(ft.done())
        self.assertEqual(TaskStatus.COMPLETE, ft.status)
        self.assertEqual(fs_len - 1, len(fs))

        n = 10
        fs_len = len(fs)
        count = 0
        for ft in self.eq_sql.as_completed(fs, pop=True, n=n):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len - count, len(fs))

        self.assertEqual(fs_len - n, len(fs))
        self.eq_sql.close()

    def test_as_completed_pop_batch(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        # 100 submissions
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 100 results
        for _ in range(100):
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        fs_len = len(fs)
        ft = self.eq_sql.pop_completed(fs)
        self.assertTrue(ft.done())
        self.assertEqual(TaskStatus.COMPLETE, ft.status)
        self.assertEqual(fs_len - 1, len(fs))

        n = 30
        fs_len = len(fs)
        count = 0
        ft_task_ids = set()
        for ft in self.eq_sql.as_completed(fs, pop=True, n=n, batch_size=12):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len - count, len(fs))
            ft_task_ids.add(ft.eq_task_id)

        # test getting no duplicates
        self.assertEqual(count, len(ft_task_ids))
        self.assertEqual(fs_len - n, len(fs))
        self.assertEqual(n, count)
        self.eq_sql.close()

    def test_as_completed_batch(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        # 20 submissions
        for i in range(0, 20):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 20 results
        for _ in range(20):
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        # n greater than number of futures
        n = 30
        fs_len = len(fs)
        count = 0
        ft_task_ids = set()
        for ft in self.eq_sql.as_completed(fs, pop=False, n=n, batch_size=12):
            count += 1
            self.assertTrue(ft.done())
            ft_task_ids.add(ft.eq_task_id)
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len, len(fs))

        # test getting no duplicates
        self.assertEqual(count, len(ft_task_ids))
        self.assertEqual(count, fs_len)
        self.assertNotEqual(count, n)
        self.eq_sql.close()

    def test_as_completed_batch_no_n(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        # 20 submissions
        for i in range(0, 20):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        # add 20 results
        for _ in range(20):
            result = self.eq_sql.query_task(0, timeout=0.0)
            if result['type'] == 'status':
                break
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        # n greater than number of futures
        fs_len = len(fs)
        count = 0
        ft_task_ids = set()
        for ft in self.eq_sql.as_completed(fs, pop=False, batch_size=12):
            count += 1
            self.assertTrue(ft.done())
            ft_task_ids.add(ft.eq_task_id)
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len, len(fs))

        # test getting no duplicates
        self.assertEqual(count, len(ft_task_ids))
        self.assertEqual(count, fs_len)
        self.eq_sql.close()

    def test_cancel_tasks(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        status, ids = self.eq_sql.cancel_tasks(fs)
        self.assertEqual(ResultStatus.SUCCESS, status)
        self.assertEqual(200, len(ids))

        ids_set = set(ids)
        for ft in fs:
            self.assertTrue(ft.eq_task_id in ids_set)

        for f in fs:
            self.assertEqual(TaskStatus.CANCELED, f.status)

        self.eq_sql.close()

    def test_update_priorities(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        fs = []
        for i in range(0, 200):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fs.append(ft)

        status, ids = self.eq_sql.update_priorities(fs, 10)
        self.assertEqual(ResultStatus.SUCCESS, status)
        self.assertEqual(200, len(ids))
        self.assertEqual(sorted(ids), [x for x in range(1, 201)])

        for f in fs:
            self.assertEqual(10, f.priority)

        status, ids = self.eq_sql.update_priorities(fs, [f.eq_task_id for f in fs])
        self.assertEqual(ResultStatus.SUCCESS, status)
        self.assertEqual(200, len(ids))
        self.assertEqual(sorted(ids), [x for x in range(1, 201)])

        for f in fs:
            self.assertEqual(f.eq_task_id, f.priority)

        # pop off output queue
        for i in range(0, 10):
            self.eq_sql.query_task(0, timeout=0.5)

        running_fs = fs[-10:]
        # update their priority, should have no effect because
        # they are already running
        self.eq_sql.update_priorities(running_fs, 22)
        for f in fs:
            # priority remains their task_id, not 22
            self.assertEqual(f.eq_task_id, f.priority)

        self.eq_sql.close()

    def test_queues_empty(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        self.assertTrue(self.eq_sql.are_queues_empty())

        # Add to output queue
        for i in range(5):
            payload = create_payload(i)
            submit_status, _ = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

        self.assertFalse(self.eq_sql.are_queues_empty())

        # Remove from output queue by grabbing for work
        task_ids = []
        for _ in range(5):
            result = self.eq_sql.query_task(0, timeout=0.0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_ids.append(task_id)

        self.assertTrue(self.eq_sql.are_queues_empty())

        # Add to input queue
        for task_id in task_ids:
            task_result = {'j': task_id}
            report_result = self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        self.assertFalse(self.eq_sql.are_queues_empty())
        clear_db()

        self.assertTrue(self.eq_sql.are_queues_empty())

        # Add to output queue
        for i in range(5):
            payload = create_payload(i)
            submit_status, _ = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

        self.assertTrue(self.eq_sql.are_queues_empty(eq_type=1))
        self.eq_sql.submit_task('eq_test', 1, create_payload(5), priority=0)
        self.assertFalse(self.eq_sql.are_queues_empty(eq_type=1))

    def test_clear_queues(self):
        self.eq_sql = local_queue.init_task_queue(host, user, port, db_name, password)
        clear_db()

        # Add to output queue
        fts = []
        for i in range(5):
            payload = create_payload(i)
            submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)
            fts.append(ft)

        # Add some to input queue by reporting result
        for _ in range(3):
            result = self.eq_sql.query_task(0, timeout=0.0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            self.eq_sql.report_task(task_id, 0, json.dumps(task_result))

        for ft in fts:
            self.assertTrue(ft.status != TaskStatus.CANCELED)

        self.assertFalse(self.eq_sql.are_queues_empty())
        self.eq_sql.clear_queues()
        self.assertTrue(self.eq_sql.are_queues_empty())

        # TODO: Only those that were in the queues are canceled
        # so some are will be complete (via the code above)
        for ft in fts:
            exp_status = TaskStatus.COMPLETE if ft.eq_task_id < 4 else TaskStatus.CANCELED
            self.assertEqual(exp_status, ft.status)


class CFGTests(unittest.TestCase):

    def test_cfg(self):
        f = "test_data/cfg_ex.yaml"
        params = parse_yaml_cfg(f)

        # ~/Documents/eqsql_dbs/db1
        self.assertEqual(os.path.expanduser('~/Documents/eqsql_dbs/db1'), params['db_path'])

        abs_f = os.path.abspath(f)
        abs_d = os.path.dirname(abs_f)

        #  ../swift/run_eqsql_workflow.sh
        exp = os.path.dirname(abs_d)
        self.assertEqual(f'{exp}/swift/run_eqsql_workflow.sh', params['pool_launch_script'])
        # ./output/
        self.assertEqual(f"{abs_d}/output", params['out_path'])
        # /home/nick/Documents/repos/../emews_examples/simple_eqsql/swift/cfgs/eqsql_workflow.cfg
        self.assertEqual('/home/nick/Documents/emews_examples/simple_eqsql/swift/cfgs/eqsql_workflow.cfg',
                         params['pool_cfg_file'])
