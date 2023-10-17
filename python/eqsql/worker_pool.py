from datetime import datetime
from typing import Dict, Union
from subprocess import Popen, STDOUT, PIPE
from time import sleep
from psij.job_status import JobStatus, JobState
from globus_compute_sdk import Executor

import psutil
import os


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

    def __init__(self, name: str, proc: Popen, cfg_file):
        """Encapsulates a locally running  EQSQL worker pool. LocalPool
        instances should **only** be created using :py:func:`start_local_pool`.

        Args:
            name: the name of the worker pool
            proc: the POpen instance used to launch the pool.
            cfg_file: the configuration file used to launch this pool
        """
        self.name = name
        self.proc = proc
        self.cfg_file = cfg_file
        self.canceled = False

    def cancel(self, timeout=10):
        """Cancels this worker pool.

        Args:
            timeout: the attempt to cancel will timeout after this duration.
        """
        p = psutil.Process(self.proc.pid)

        with self.proc:
            if p.is_running():
                for child_process in p.children(recursive=True):
                    if child_process.is_running():
                        child_process.send_signal(15)

            # may have terminated during the above
            if p.is_running():
                self.proc.terminate()

            retry_count = 0
            sleep_val = 0.25
            while self.proc.poll() is None and retry_count < timeout / sleep_val:
                sleep(sleep_val)

        self.canceled = True

    def status(self) -> JobStatus:
        """Gets the status (active, completed, or canceled) of this LocalPool.

        Returns:
            The current JobStatus of this LocalPool with JobState
            attribute set appropriately - ``JobState.CANCELED``,
            ``JobState.ACTIVE``, or ``JobState.COMPLETE``.

        Examples:
            >>> pool.status().state
            JobState.ACTIVE
        """
        if self.canceled:
            return JobStatus(JobState.CANCELED)

        rc = self.proc.poll()
        if rc is None:
            return JobStatus(JobState.ACTIVE)
        elif rc == 0:
            return JobStatus(JobState.COMPLETED)
        else:
            return JobStatus(JobState.FAILED)


class ScheduledPool:

    def __init__(self, name, job_id, scheduler, gcx, cfg_file):
        self.job_id = job_id
        self.name = name
        self.scheduler = scheduler
        self.cfg_file = cfg_file
        self.gcx = gcx

    def cancel(self, timeout=60):
        """Cancels this worker pool.

        Args:
            timeout: the attempt to cancel will timeout after this duration.
        """
        if self.gcx is None:
            _cancel_pool(self.job_id, self.scheduler)
        else:
            ft = self.gcx.submit(_cancel_pool, self.job_id, self.scheduler)
            ft.result()

        retry_count = 0
        sleep_val = 0.25
        # TODO: fix this so polls for canceled some number of times, then returns a value.s
        while self.status().state != JobState.CANCELED and retry_count < timeout / sleep_val:
            sleep(sleep_val)
            retry_count += 1

    def status(self, timeout=60):
        """Gets the status (active, completed, or canceled) of this LocalPool.

        Returns:
            The current JobStatus of this LocalPool with JobState
            attribute set appropriately - ``JobState.CANCELED``,
            ``JobState.ACTIVE``, or ``JobState.COMPLETE``.

        Examples:
            >>> pool.status().state
            JobState.ACTIVE
        """
        if self.gcx is None:
            return _pool_status(self.job_id, self.scheduler)
        else:
            ft = self.gcx.submit(_pool_status, self.job_id, self.scheduler)
            return ft.result(timeout=timeout)


def cfg_tofile(cfg_params: Dict) -> str:
    import tempfile
    import os
    fd, fname = tempfile.mkstemp(text=True)
    with os.fdopen(fd, 'w') as f:
        for k, v in cfg_params.items():
            f.write(f'{k}={v}\n')
    return fname


def _coerce(value: str):
    try:
        v = float(value)
        if int(v) == v:
            v = int(v)
        return v
    except ValueError:
        return value


def cfg_file_to_dict(cfg_file: str) -> Dict:
    """Reads a bash cfg file into a dictionary"""
    params = {}
    with open(cfg_file) as fin:
        for line in fin.readlines():
            line = line.strip()
            if not line.startswith('#'):
                kv = line.split('=')
                if len(kv) == 2:
                    k, v = kv
                    params[k] = _coerce(v)

    return params


def start_local_pool(name: str, launch_script: Union[str, bytes, os.PathLike],
                     exp_id: str, cfg_params: Dict) -> LocalPool:
    """Starts a local worker pool and returns the LocalPool instance encapsulating
    that worker pool.

    Args:
        name: the name of the worker pool
        launch_script: the path to an executable script used to start the worker pool
        exp_id: the experiment id for the experiment that is running the pool
        cfg_params: the parameters used to start the pool
    Returns:
        A LocalPool instance.
    """
    cfg_params['CFG_POOL_ID'] = name
    cfg_fname = cfg_tofile(cfg_params)
    exp_id = format_pool_exp_id(exp_id, name)
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


def start_scheduled_pool(name: str, launch_script: Union[str, bytes, os.PathLike],
                         exp_id: str, cfg_params: Dict, scheduler: str, gcx: Executor):
    """Starts a worker pool on a scheduled resource, i.e., a resource that queues jobs
    using some sort of scheduler, e.g., slurm.

    Args:
        name: the name of the worker pool
        launch_script: the path to an executable script used to start the worker pool
        exp_id: the experiment id for the experiment that is running the pool
        cfg_params: the parameters used to start the pool
        scheduler: scheduled resource schedule type - slurm, etc.
        gcx: a globus compute Executor instance. If this is None, then the assumption is that the call
            to this function is running on the resource where the pool is to be launched. If
            this is not None, then the executor will be used to launch the pool remotely.

    Returns:
        An instance of a ScheduledPool object.
    """
    def _start_scheduled_pool(launch_script, exp_id, cfg_params):
        # imports here for funcx
        import os
        import subprocess
        import re
        import traceback
        from eqsql import worker_pool

        fname = worker_pool.cfg_tofile(cfg_params)
        try:
            cwd = os.path.dirname(launch_script)
            exp_id = format_pool_exp_id(exp_id, name)
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

    cfg_params['CFG_POOL_ID'] = name
    if gcx is None:
        job_id, cfg_file = _start_scheduled_pool(launch_script, exp_id, cfg_params)
    else:
        ft = gcx.submit(_start_scheduled_pool, launch_script, exp_id, cfg_params)
        job_id, cfg_file = ft.result()
    return ScheduledPool(name, job_id, scheduler, gcx, cfg_file)
