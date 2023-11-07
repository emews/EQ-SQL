import unittest
import json
from globus_compute_sdk import Executor

from eqsql.task_queues import remote
from eqsql.task_queues.core import ResultStatus, TaskStatus, TimeoutError
from eqsql.task_queues.core import EQ_TIMEOUT, EQ_STOP, EQ_ABORT

# Assumes the existence of a testing database
# with these characteristics
host = 'beboplogin1'
user = 'eqsql_test_user'
port = 52718
db_name = 'eqsql_test_db'

# bebop gce_py3.10
gcx_endpoint = '2b2fa624-9845-494b-8ba8-2750821d3716'


def create_payload(x=1.2):
    payload = {'x': x, 'y': 7.3, 'z': 'foo'}
    return json.dumps(payload)


def report_task(db_params, eq_task_id: int, eq_type: int, result: str) -> ResultStatus:
    from eqsql.task_queues import local
    task_queue = local.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                       retry_threshold=db_params.retry_threshold)
    return task_queue.report_task(eq_task_id, eq_type, result)


def query_task(db_params, eq_type, n: int = 1, worker_pool: str = 'default', delay: float = 0.5,
               timeout: float = 2.0):
    from eqsql.task_queues import local
    task_queue = local.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                       retry_threshold=db_params.retry_threshold)
    return task_queue.query_task(eq_type, n, worker_pool, delay, timeout)


def clear_db(gcx: Executor):
    def _reset_db(db_user: str = 'eqsql_test_user', dbname: str = 'eqsql_test_db', db_host: str = 'localhost',
                  db_port: int = None):
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
            self.tq = remote.init_task_queue(gcx, host, user, port, db_name)
            result_status, ft = self.tq.submit_task('test_future', 0, create_payload(), tag='x')
            self.assertEqual(ResultStatus.SUCCESS, result_status)
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertFalse(ft.done())
            self.assertEqual('x', ft.tag)

    def test_query_priority(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = remote.init_task_queue(gcx, host, user, port, db_name)
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

    def test_query_result(self):
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = remote.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            payload = create_payload()
            _, ft = self.eq_sql.submit_task('test_future', 0, payload)
            self.assertEqual(TaskStatus.QUEUED, ft.status)
            self.assertIsNone(ft.worker_pool)
            self.assertFalse(ft.done())
            result_status, result = ft.result(timeout=0.5)
            self.assertEqual(ResultStatus.FAILURE, result_status)
            self.assertEqual(result, EQ_TIMEOUT)

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
            # self.eq_sql.report_task(task_id, 0, json.dumps(task_result))
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
        with Executor(endpoint_id=gcx_endpoint) as gcx:
            self.eq_sql = remote.init_task_queue(gcx, host, user, port, db_name)
            clear_db(gcx)

            fs = []
            for i in range(0, 200):
                payload = create_payload(i)
                submit_status, ft = self.eq_sql.submit_task('eq_test', 0, payload, priority=0)
                self.assertEqual(ResultStatus.SUCCESS, submit_status)
                fs.append(ft)

            status, count = self.eq_sql.cancel_tasks(fs)
            self.assertEqual(ResultStatus.SUCCESS, status)
            self.assertEqual(200, count)

            for f in fs:
                self.assertEqual(TaskStatus.CANCELED, f.status)
