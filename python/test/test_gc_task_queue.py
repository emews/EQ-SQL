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
