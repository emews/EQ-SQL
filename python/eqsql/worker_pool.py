from datetime import datetime
from typing import Dict
from subprocess import Popen, STDOUT, PIPE, CalledProcessError
from time import sleep
from psij.job_status import JobStatus
import psutil


def format_pool_exp_id(exp_id: str, name: str):
    dt = datetime.now()
    ts = datetime.timestamp(dt)
    return f'{exp_id}-{name}_{ts}'


def _pool_status(job_id, scheduler, poll_period=5) -> JobStatus:
    # imports here for funcx
    from psij import Job, JobExecutor
    import time

    executor = JobExecutor.get_instance(scheduler)
    job = Job()
    executor.attach(job, job_id)
    time.sleep(poll_period)
    return job.status


def _cancel_pool(job_id, scheduler, poll_period=5):
    # imports here for funcx
    from psij import Job, JobExecutor
    import time

    executor = JobExecutor.get_instance(scheduler)
    job = Job()
    executor.attach(job, job_id)
    time.sleep(poll_period)
    job.cancel()


class LocalPool:

    def __init__(self, name, proc, cfg_file):
        self.name = name
        self.proc = proc
        self.cfg_file = cfg_file

    def cancel(self):
        with self.proc:
            pid = self.proc.pid
            p = psutil.Process(pid)
            for child_process in p.children(recursive=True):
                child_process.send_signal(15)
            self.proc.terminate()


class ScheduledPool:

    def __init__(self, name, job_id, scheduler, cfg_file):
        self.job_id = job_id
        self.name = name
        self.scheduler = scheduler
        self.cfg_file = cfg_file

    def cancel(self, fx):
        ft = fx.submit(_cancel_pool, self.job_id, self.scheduler)
        ft.result()

    def status(self, fx, timeout=60):
        ft = fx.submit(_pool_status, self.job_id, self.scheduler)
        return ft.result(timeout=timeout)


def cfg_tofile(cfg_params: Dict) -> str:
    import tempfile
    import os
    fd, fname = tempfile.mkstemp(text=True)
    with os.fdopen(fd, 'w') as f:
        for k, v in cfg_params.items():
            if k.startswith('CFG'):
                f.write(f'{k}={v}\n')
    return fname

# def _start_local_p


def start_local_pool(name, launch_script, exp_id, cfg_params):
    cfg_fname = cfg_tofile(cfg_params)
    # try:
    proc = Popen([launch_script, str(exp_id), cfg_fname], stdout=PIPE,
                 stderr=STDOUT)
    for _ in range(4):
        sleep(2)
        rc = proc.poll()
        if rc is not None:
            stdout, _ = proc.communicate()
            raise ValueError(f"start_local_pool failed with {stdout.decode('utf-8')}")

    # assume started and running
    return LocalPool(name, proc, cfg_fname)

def start_scheduled_pool(fx, name, launch_script, exp_id, cfg_params, scheduler):
    def _start_scheduled_pool(launch_script, exp_id, cfg_params, scheduler):
        # imports here for funcx
        import os
        import subprocess
        import re
        import traceback
        from eqsql import worker_pool

        fname = worker_pool.cfg_tofile(cfg_params)
        try:
            cwd = os.path.dirname(launch_script)
            result = subprocess.run([launch_script, str(exp_id), fname], stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, cwd=cwd, check=True)
            result_str = result.stdout.decode('utf-8')
            match = re.search(".*^JOB_ID=([0-9]*)", result_str, re.MULTILINE)
            if match is None:
                raise ValueError(f'start_scheduled_pool job id match failed with {result_str}')
            job_id = match[1]
            return job_id, fname

        except subprocess.CalledProcessError:
            raise ValueError(f'start_scheduled_pool failed with {traceback.format_exc()}')

    ft = fx.submit(_start_scheduled_pool, launch_script, exp_id, cfg_params, scheduler)
    job_id, cfg_file = ft.result()
    return ScheduledPool(name, job_id, scheduler, cfg_file)
