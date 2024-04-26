import unittest
import json
from globus_compute_sdk import Executor

from eqsql.task_queues import gc_queue
from eqsql.task_queues.core import ResultStatus, TaskStatus, TimeoutError
from eqsql.task_queues.core import EQ_TIMEOUT

from .common import create_payload, report_task, query_task

# Database - DB_DATA: /lcrc/project/EMEWS/ncollier/eqsql_db
# Postgres: /lcrc/project/EMEWS/bebop/sfw/gcc-7.1.0/postgres-14.2
# Start db: nice -n 19 pg_ctl -D $DB_DATA -l $DB_DATA/db.log -o "-F -p 52718" start
# CLI: psql -h beboplogin3 -p 52718 -U eqsql_test_user -d eqsql_test_db

# Assumes the existence of a testing database
# with these characteristics
host = 'ilogin3'
user = 'eqsql_user'
port = 52718
db_name = 'EQ_SQL'

# globus compute endpoint - osprey-py3.10, env - osprey-py3.10
gcx_endpoint = '8d5f8bde-8c4b-48ac-9c43-0d377d68e651'


def clear_db(gcx: Executor):
    def _reset_db(db_user: str = 'eqsql_user', dbname: str = 'EQ_SQL', db_host: str = 'ilogin3',
                  db_port: int = 52718):
        import psycopg2
        """Resets the database by deleting the contents of all the eqsql tables and restarting
        the emews task id generator sequence.

        Args:
            db_user: the database user name
            db_name: the name of the database
            db_host: the hostname where the database server is located
            db_port: the port of the database server.
        """
        clear_db_sql = """
            delete from eq_exp_id_tasks;
            delete from eq_tasks;
            delete from emews_queue_OUT;
            delete from emews_queue_IN;
            delete from eq_task_tags;
            alter sequence emews_id_generator restart;
        """

        conn = psycopg2.connect(f'dbname={dbname}', user=db_user, host=db_host, port=db_port)
        with conn:
            with conn.cursor() as cur:
                cur.execute(clear_db_sql)

        conn.close()

    gc_ft = gcx.submit(_reset_db, user, db_name, host, port)
    gc_ft.result()


class GCTaskQueueTests(unittest.TestCase):

    def test_submit_task(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            clear_db(gcx)
            self.tq = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            result_status, ft = self.tq.submit_task('test_future', 0, create_payload(), tag='x')
            self.assertEqual(ResultStatus.SUCCESS, result_status)
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())
            self.assertEqual('x', ft.tag)

    def test_query_priority(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)
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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

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

    def test_query_result(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payload = create_payload()
            _, ft = self.eq_sql.submit_task('test_future', 0, payload)
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertIsNone(ft.worker_pool)
            self.assertFalse(ft.done())
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(ResultStatus.FAILURE, result_status)
            self.assertEqual(result, EQ_TIMEOUT)

            # mimic worker pool querying for work
            result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0.5).result()
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
            report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                       eq_type=0, result=json.dumps(task_result)).result()
            self.assertEqual(ResultStatus.SUCCESS, report_result)

            # test get result
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(ResultStatus.SUCCESS, result_status)
            self.assertEqual(task_result, json.loads(result))

            # test status
            task_status = ft.status
            self.assertEqual(TaskStatus.COMPLETE, task_status)
            self.assertTrue(ft.done())

    def test_cancel_tasks(self):
        # from datetime import datetime
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            # print(f'a: {datetime.now()}', flush=True)
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            # print(f'b: {datetime.now()}', flush=True)
            clear_db(gcx)
            # print(f'c: {datetime.now()}', flush=True)

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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

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
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, worker_pool='P1',
                                    timeout=0).result()
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

    def test_as_completed(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payloads = [create_payload(i) for i in range(0, 30)]
            submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

            timedout = False
            try:
                for ft in self.eq_sql.as_completed(fts, timeout=5):
                    pass
                self.fail('timeout exception expected')
            except TimeoutError:
                timedout = True
            self.assertTrue(timedout)

            # Add 2 results as if worker pool had done them
            for _ in range(2):
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
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
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payloads = [create_payload(i) for i in range(0, 30)]
            submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

            # Add 10 results as if worker pool had done them
            for _ in range(10):
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payloads = [create_payload(i) for i in range(0, 40)]
            submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

            # Add 35 results as if worker pool had done them
            for _ in range(35):
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
                self.assertEqual(ResultStatus.SUCCESS, report_result)

            fs_len = len(fts)
            n = 30
            count = 0
            for ft in self.eq_sql.as_completed(fts, timeout=None, n=n, pop=True, batch_size=12):
                count += 1
                self.assertTrue(ft.done())
                self.assertEqual(TaskStatus.COMPLETE, ft.status)
                self.assertEqual(fs_len - count, len(fts))
                self.assertTrue(ft not in fts)

            self.assertEqual(fs_len - n, len(fts))

    def test_as_completed_batch(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payloads = [create_payload(i) for i in range(0, 20)]
            submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

            # Add 20 results (< n) as if worker pool had done them
            for _ in range(20):
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payloads = [create_payload(i) for i in range(0, 20)]
            submit_status, fts = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

            # Add 20 results as if worker pool had done them
            for _ in range(20):
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
                self.assertEqual(ResultStatus.SUCCESS, report_result)

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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = gc_queue.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            self.assertTrue(self.eq_sql.are_queues_empty())

            # Add to output queue
            payloads = [create_payload(i) for i in range(0, 5)]
            submit_status, _ = self.eq_sql.submit_tasks('eq_test', 0, payloads, priority=0)
            self.assertEqual(ResultStatus.SUCCESS, submit_status)

            self.assertFalse(self.eq_sql.are_queues_empty())

            task_ids = []
            for _ in range(5):
                result = gcx.submit(query_task, self.eq_sql.db_params, eq_type=0, timeout=0).result()
                self.assertEqual('work', result['type'])
                task_id = result['eq_task_id']
                task_ids.append(task_id)

            self.assertTrue(self.eq_sql.are_queues_empty())

            # Add to input queue
            for task_id in task_ids:
                task_result = {'j': task_id}
                report_result = gcx.submit(report_task, self.eq_sql.db_params, eq_task_id=task_id,
                                           eq_type=0, result=json.dumps(task_result)).result()
                self.assertEqual(ResultStatus.SUCCESS, report_result)

            self.assertFalse(self.eq_sql.are_queues_empty())
            clear_db(gcx)

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
