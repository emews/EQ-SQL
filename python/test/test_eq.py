import unittest
import os
import json

from eqsql import eq


os.environ['DB_HOST'] = 'localhost'
os.environ['DB_USER'] = 'eqsql_test_user'
os.environ['DB_PORT'] = '5433'
os.environ['DB_NAME'] = 'eqsql_test_db'


def create_payload():
    payload = {'x': 1.2, 'y': 7.3, 'z': 'foo'}
    return json.dumps(payload)


clear_db_sql = """
delete from eq_exp_id_tasks;
delete from eq_tasks;
delete from emews_queue_OUT;
delete from emews_queue_IN;
alter sequence emews_id_generator restart;
"""


def clear_db(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(clear_db_sql)


class EQTests(unittest.TestCase):

    def test_submit(self):
        eq.init()
        clear_db(eq.DB.conn)
        result_status, ft = eq.submit_task('test_future', 0, create_payload())
        self.assertEqual(eq.ResultStatus.SUCCESS, result_status)
        self.assertEqual(eq.TaskStatus.QUEUED, ft.status())

        eq.close()
