import unittest
import yaml
import funcx
from time import sleep
from psij.job_state import JobState


from eqsql import worker_pool

bebop_ep = 'd526418b-8920-4bc9-a9a0-3c97e1a10d3b'

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

    def test_scheduled_pool(self):
        params = yaml.safe_load(scheduled_pool_yaml)
        exp_id = worker_pool.format_pool_exp_id('t1', 'bebop1')
        with funcx.FuncXExecutor(endpoint_id=bebop_ep) as fx:
            pool = worker_pool.start_scheduled_pool(fx, 'bebop1', params['start_pool_script'],
                                                    exp_id, params, 'slurm')
            self.assertEqual('bebop1', pool.name)
            self.assertIsNotNone(pool.job_id)
            # 2 should be good for a while
            self.assertTrue(pool.job_id.startswith('2'))
            sleep(4)
            self.assertEqual(JobState.ACTIVE, pool.status(fx).state)
            pool.cancel(fx)
            # sleep needs longer than this
            # sleep(10)
            # self.assertEqual(JobState.CANCELED, pool.status(fx).state)

    def test_local_pool(self):
        params = yaml.safe_load(local_pool_yaml)
        name = 'local1'
        exp_id = worker_pool.format_pool_exp_id('t1', name)
        pool = worker_pool.start_local_pool(name, params['start_pool_script'],
                                            exp_id, params)
        # TODO: replace with status
        rc = pool.proc.poll()
        self.assertIsNone(rc)
        pool.cancel()
        sleep(2)
        rc = pool.proc.poll()
        self.assertIsNotNone(rc)
