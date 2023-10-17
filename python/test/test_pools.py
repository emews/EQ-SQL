import unittest
import yaml
from time import sleep
import json
import os

from psij.job_state import JobState

from eqsql import worker_pool, eq
from .utils import clear_db

# Assumes the existence of a testing database
# with these characteristics
host = 'localhost'
user = 'eqsql_test_user'
port = 5433
db_name = 'eqsql_test_db'

bebop_ep = '2b2fa624-9845-494b-8ba8-2750821d3716'

scheduled_pool_yaml = '''
start_pool_script: /lcrc/project/EMEWS/bebop/repos/EQ-SQL/python/test_data/test_swift_submit.sh
CFG_PPN: 36
CFG_NODES: 1
CFG_PROCS: $(( CFG_NODES * CFG_PPN ))
CFG_BATCH_SIZE: $(( CFG_PROCS - 3 ))
CFG_BATCH_THRESHOLD: 15
CFG_WALLTIME: 01:00:00
CFG_QUEUE: dis
CFG_PROJECT: CONDO
'''

local_pool_yaml = '''
start_pool_script: ./test_data/test_local_swift_submit.sh
CFG_PPN: 4
CFG_NODES: 1
CFG_PROCS: $(( CFG_NODES * CFG_PPN ))
'''


class PoolTests(unittest.TestCase):

    def test_cfgf_to_dict(self):
        exp = yaml.safe_load(scheduled_pool_yaml)
        del exp['start_pool_script']
        fname = './test_data/tmp.cfg'
        with open(fname, 'w') as fout:
            for k, v in exp.items():
                fout.write(f'{k}={v}\n')

        params = worker_pool.cfg_file_to_dict(fname)
        self.assertEqual(len(params), len(exp))
        for p in params:
            self.assertTrue(p in exp)
            self.assertEqual(exp[p], params[p])

        os.remove(fname)

    def test_scheduled_pool(self):
        from globus_compute_sdk import Executor
        params = yaml.safe_load(scheduled_pool_yaml)
        exp_id = worker_pool.format_pool_exp_id('t1', 'bebop1')
        with Executor(endpoint_id=bebop_ep) as gcx:
            pool = worker_pool.start_scheduled_pool('bebop1', params['start_pool_script'],
                                                    exp_id, params, 'slurm', gcx)
            self.assertEqual('bebop1', pool.name)
            self.assertIsNotNone(pool.job_id)
            # 7 should be good for a while
            self.assertEqual(7, len(pool.job_id))
            sleep(4)
            self.assertEqual(JobState.ACTIVE, pool.status().state)
            pool.cancel()
            # sleep needs longer than this
            # sleep(10)
            # self.assertEqual(JobState.CANCELED, pool.status(fx).state)

    def test_scheduled_pool_local(self):
        params = yaml.safe_load(scheduled_pool_yaml)
        exp_id = worker_pool.format_pool_exp_id('t1', 'bebop1')
        pool = worker_pool.start_scheduled_pool('bebop1', params['start_pool_script'],
                                                exp_id, params, 'slurm', gcx=None)
        self.assertEqual('bebop1', pool.name)
        self.assertIsNotNone(pool.job_id)
        # 7 should be good for a while
        self.assertEqual(7, len(pool.job_id))
        sleep(30)
        self.assertEqual(JobState.ACTIVE, pool.status().state)
        pool.cancel()
        # sleep needs longer than this
        # sleep(10)
        # self.assertEqual(JobState.CANCELED, pool.status(fx).state)

    def test_local_pool(self):
        # make sure swift-t in path
        params = yaml.safe_load(local_pool_yaml)
        name = 'local1'
        exp_id = worker_pool.format_pool_exp_id('t1', name)
        pool = worker_pool.start_local_pool(name, params['start_pool_script'],
                                            exp_id, params)

        state = pool.status().state
        self.assertEqual(JobState.ACTIVE, state)
        pool.cancel()
        sleep(2)
        state = pool.status().state
        self.assertEqual(JobState.CANCELED, state)

    def test_cancel_pool(self):
        # 1. start pool
        # 2. schedule tasks
        # 3. query tasks to remove N from queue
        # 4. cancel pool
        # 5. old futures should have rescheduled status
        # 6. N new futures
        # make sure swift-t in path
        params = yaml.safe_load(local_pool_yaml)
        name = 'local1'
        exp_id = worker_pool.format_pool_exp_id('t1', name)
        pool = worker_pool.start_local_pool(name, params['start_pool_script'],
                                            exp_id, params)

        state = pool.status().state
        self.assertEqual(JobState.ACTIVE, state)

        try:
            self.eq_sql = eq.init_task_queue(host, user, port, db_name)
            clear_db(self.eq_sql.db.conn)
            exp_id = '1'

            fts = []
            payloads = {}
            tags = {}
            for i in range(10):
                payload = json.dumps({'x': i})
                _, ft = self.eq_sql.submit_task(exp_id, 0, payload, priority=i, tag=f'tag {i}')
                fts.append(ft)
                self.assertEqual(eq.TaskStatus.QUEUED, ft.status)
                payloads[ft.eq_task_id] = payload
                tags[ft.eq_task_id] = f'tag {i}'
                self.assertFalse(ft.done())

            self.eq_sql.query_task(0, n=2, worker_pool=name)

            running = []
            for ft in fts:
                if ft.status == eq.TaskStatus.RUNNING:
                    self.assertEqual(name, ft.worker_pool)
                    running.append(ft)
            self.assertEqual(2, len(running))

            eq_env = eq.EQEnvironment(exp_id)
            eq_env._pq_map[name] = self.eq_sql

            new_fts = eq.cancel_worker_pool(pool, eq_env, self.eq_sql, fts)
            self.assertEqual(len(fts), len(new_fts))
            self.assertTrue(running[0] not in new_fts)
            self.assertTrue(running[1] not in new_fts)
            self.assertEqual(eq.TaskStatus.REQUEUED, running[0].status)
            self.assertEqual(eq.TaskStatus.REQUEUED, running[1].status)

            # first two should be the new ones
            ft1, ft2 = new_fts[:2]
            self.assertTrue(ft1 not in fts)
            self.assertTrue(ft2 not in fts)
            self.assertEqual(eq.TaskStatus.QUEUED, ft1.status)
            self.assertEqual(eq.TaskStatus.QUEUED, ft2.status)
            self.assertFalse(ft1.done())
            self.assertFalse(ft1.done())

            exp_payloads = (payloads[running[0].eq_task_id], payloads[running[1].eq_task_id])
            ft1_payload = self.eq_sql._get("select json_out from eq_tasks where eq_task_id = %s", ft1.eq_task_id)[1][0][0]
            idx1 = exp_payloads.index(ft1_payload)
            ft2_payload = self.eq_sql._get("select json_out from eq_tasks where eq_task_id = %s", ft2.eq_task_id)[1][0][0]
            idx2 = exp_payloads.index(ft2_payload)
            self.assertNotEqual(idx1, idx2)

            priorities = [running[0].priority, running[1].priority]
            self.assertTrue(ft1.priority in priorities)
            self.assertTrue(ft2.priority in priorities)
            self.assertTrue(ft1.priority != ft2.priority)

            exp_tags = (tags[running[0].eq_task_id], tags[running[1].eq_task_id])
            ft1_tag = self.eq_sql._get("select tag from eq_task_tags where eq_task_id = %s", ft1.eq_task_id)[1][0][0]
            ft2_tag = self.eq_sql._get("select tag from eq_task_tags where eq_task_id = %s", ft2.eq_task_id)[1][0][0]
            self.assertTrue(ft1_tag in exp_tags)
            self.assertTrue(ft2_tag in exp_tags)
            self.assertTrue(ft1_tag != ft2_tag)

        except Exception as e:
            pool.cancel()
            raise e
