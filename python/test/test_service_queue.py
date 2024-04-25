import unittest
import json
import os
import shutil

from eqsql.task_queues import service_queue
from eqsql.task_queues.core import ResultStatus, TaskStatus, TimeoutError
from eqsql.task_queues.core import EQ_TIMEOUT
from eqsql.task_queues.remote_funcs import DBParameters
from eqsql.db_tools import reset_db, init_eqsql_db, start_db, stop_db, is_db_running

from .common import create_payload, report_task, query_task

host = 'localhost'
user = 'eqsql_test_user'
port = 5444
db_name = 'eqsql_test_db'
password = None

db_path = './test_data/db/eqsql_test'
pg_bin = '/home/nick/sfw/postgresql-14.11/bin'

service_url = 'http://127.0.0.1:5000'


def clear_db():
    reset_db(user, db_name, host, port, password)


class ServiceTests(unittest.TestCase):

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

    def test_submit_task(self):
        clear_db()

        self.eqsql = service_queue.init_task_queue(service_url, host, user, port,
                                                   db_name)
        result_status, ft = self.eqsql.submit_task('test_future', 0, create_payload(), tag='x')
        self.assertEqual(ResultStatus.SUCCESS, result_status)
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertFalse(ft.done())
        self.assertEqual('x', ft.tag)

    def test_query_result(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port,
                                                    db_name)
        clear_db()

        payload = create_payload()
        _, ft = self.eq_sql.submit_task('test_future', 0, payload)
        self.assertEqual(TaskStatus.QUEUED, ft.status)
        self.assertIsNone(ft.worker_pool)
        self.assertFalse(ft.done())
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(ResultStatus.FAILURE, result_status)
        self.assertEqual(result, EQ_TIMEOUT)

        # mimic worker pool querying for work
        result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0.5)
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
        report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                    eq_type=0, result=json.dumps(task_result))
        self.assertEqual(ResultStatus.SUCCESS, report_result)

        # test get result
        result_status, result = ft.result(timeout=0.5)
        self.assertEqual(ResultStatus.SUCCESS, result_status)
        self.assertEqual(task_result, json.loads(result))

        # test status
        task_status = ft.status
        self.assertEqual(TaskStatus.COMPLETE, task_status)
        self.assertTrue(ft.done())

    def test_as_completed(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port,
                                                    db_name)
        clear_db()

        payloads = [create_payload(i) for i in range(0, 30)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        timedout = False
        try:
            for _ in self.eq_sql.as_completed(fts, timeout=5):
                pass
            self.fail('timeout exception expected')
        except TimeoutError:
            timedout = True
        self.assertTrue(timedout)

        # Add 2 results as if worker pool had done them
        for _ in range(2):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        count = 0
        for ft in self.eq_sql.as_completed(fts, timeout=None, n=1):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            status, result_str = ft.result(timeout=0)
            self.assertEqual(ResultStatus.SUCCESS, status)
            self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])

        timedout = False
        try:
            for ft in self.eq_sql.as_completed(fts, timeout=5, n=10):
                pass
            self.fail('timeout exception expected')
        except TimeoutError:
            timedout = True
        self.assertTrue(timedout)

        for _ in range(10):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        for ft in self.eq_sql.as_completed(fts, timeout=None, n=9):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            status, result_str = ft.result(timeout=0)
            self.assertEqual(ResultStatus.SUCCESS, status)
            self.assertEqual(ft.eq_task_id, json.loads(result_str)['j'])

        self.assertEqual(10, count)

    def test_as_completed_pop(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port,
                                                    db_name)
        clear_db()
        payloads = [create_payload(i) for i in range(0, 30)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # Add 2 results as if worker pool had done them
        for _ in range(10):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        fs_len = len(fts)
        n = 10
        count = 0
        for ft in self.eq_sql.as_completed(fts, timeout=None, n=n, pop=True):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len - count, len(fts))
            self.assertTrue(ft not in fts)

        self.assertEqual(fs_len - n, len(fts))

    def test_as_completed_pop_batch(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port,
                                                    db_name)
        clear_db()
        payloads = [create_payload(i) for i in range(0, 40)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # Add 35 results as if worker pool had done them
        for _ in range(35):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        fs_len = len(fts)
        n = 30
        count = 0
        ft_task_ids = set()
        for ft in self.eq_sql.as_completed(fts, timeout=None, n=n, pop=True, batch_size=12):
            count += 1
            self.assertTrue(ft.done())
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len - count, len(fts))
            self.assertTrue(ft not in fts)
            ft_task_ids.add(ft.eq_task_id)

        # test getting no duplicates
        self.assertEqual(count, len(ft_task_ids))
        self.assertEqual(fs_len - n, len(fts))

    def test_as_completed_batch(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port,
                                                    db_name)
        clear_db()
        payloads = [create_payload(i) for i in range(0, 20)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # add 20 results
        for _ in range(20):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        # n greater than number of futures
        n = 30
        fs_len = len(fts)
        count = 0
        ft_task_ids = set()
        for ft in self.eq_sql.as_completed(fts, pop=False, n=n, batch_size=12):
            count += 1
            self.assertTrue(ft.done())
            ft_task_ids.add(ft.eq_task_id)
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len, len(fts))

        # test getting no duplicates
        self.assertEqual(count, len(ft_task_ids))
        self.assertEqual(count, fs_len)
        self.assertNotEqual(count, n)

    def test_as_completed_batch_no_n(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port,
                                                    db_name)
        clear_db()
        # 20 submissions
        payloads = [create_payload(i) for i in range(0, 20)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # add 20 results
        for _ in range(20):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        # n greater than number of futures
        fs_len = len(fts)
        count = 0
        ft_task_ids = set()
        for ft in self.eq_sql.as_completed(fts, pop=False, batch_size=12):
            count += 1
            self.assertTrue(ft.done())
            ft_task_ids.add(ft.eq_task_id)
            self.assertEqual(TaskStatus.COMPLETE, ft.status)
            self.assertEqual(fs_len, len(fts))

        # test getting no duplicates
        self.assertEqual(count, len(ft_task_ids))
        self.assertEqual(count, fs_len)

    def test_queues_empty(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port, db_name)
        clear_db()

        self.assertTrue(self.eq_sql.are_queues_empty())

        # Add to output queue
        payloads = [create_payload(i) for i in range(0, 5)]
        submit_status, _ = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        self.assertFalse(self.eq_sql.are_queues_empty())

        task_ids = []
        for _ in range(5):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            task_ids.append(task_id)

        self.assertTrue(self.eq_sql.are_queues_empty())

        # Add to input queue
        for task_id in task_ids:
            task_result = {'j': task_id}
            report_result = report_task(DBParameters.from_dict(self.eq_sql.db_params), eq_task_id=task_id,
                                        eq_type=0, result=json.dumps(task_result))
            self.assertEqual(ResultStatus.SUCCESS, report_result)

        self.assertFalse(self.eq_sql.are_queues_empty())
        clear_db()

        self.assertTrue(self.eq_sql.are_queues_empty())

        # test by task type
        # add type 0
        payloads = [create_payload(i) for i in range(0, 5)]
        submit_status, _ = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # type 1 empty
        self.assertTrue(self.eq_sql.are_queues_empty(eq_type=1))
        self.eq_sql.submit_tasks('eq_test', 1, create_payload(1), priority=0)
        self.assertFalse(self.eq_sql.are_queues_empty(eq_type=1))

    def test_query_priority(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port, db_name)
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

    def test_update_priorities(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port, db_name)
        clear_db()

        payloads = [create_payload(i) for i in range(5)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        result = self.eq_sql.get_priorities(fts)
        self.assertEqual(len(fts), len(result))

        status, ids = self.eq_sql.update_priorities(fts, 10)
        self.assertEqual(ResultStatus.SUCCESS, status)
        self.assertEqual(len(fts), len(ids))

        for f in fts:
            self.assertEqual(10, f.priority)

        status, ids = self.eq_sql.update_priorities(fts, [f.eq_task_id for f in fts])
        for f in fts:
            self.assertEqual(f.eq_task_id, f.priority)

    def test_cancel_tasks(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port, db_name)
        clear_db()

        payloads = [create_payload(i) for i in range(0, 200)]
        # print(f'd: {datetime.now()}', flush=True)
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        # print(f'e: {datetime.now()}', flush=True)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # print(f'f: {datetime.now()}', flush=True)
        status, ids = self.eq_sql.cancel_tasks(fts)
        # print(f'g: {datetime.now()}', flush=True)
        self.assertEqual(ResultStatus.SUCCESS, status)
        self.assertEqual(200, len(ids))

        for f in fts:
            self.assertEqual(TaskStatus.CANCELED, f.status)

    def test_get_worker_pools(self):
        self.eq_sql = service_queue.init_task_queue(service_url, host, user, port, db_name)
        clear_db()

        payloads = [create_payload(i) for i in range(8)]
        submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
        self.assertEqual(ResultStatus.SUCCESS, submit_status)

        # not running, so no pool yet
        pools = self.eq_sql.get_worker_pools(fts)
        self.assertEqual(8, len(pools))
        for _, p in pools:
            self.assertIsNone(p)

        # Query for work as if from worker pool 'P1'
        p1_ids = []
        for _ in range(4):
            result = query_task(DBParameters.from_dict(self.eq_sql.db_params), eq_type=0, worker_pool='P1',
                                timeout=0)
            self.assertEqual('work', result['type'])
            task_id = result['eq_task_id']
            p1_ids.append(task_id)

        pools = self.eq_sql.get_worker_pools(fts)
        self.assertEqual(8, len(pools))
        for ft, p in pools:
            if ft.eq_task_id in p1_ids:
                self.assertEqual('P1', p)
            else:
                self.assertIsNone(p)
