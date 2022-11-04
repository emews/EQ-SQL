""" Utility functions for interfacing swift code with database queues"""
import logging
import traceback
import threading
import time
import multiprocessing as mp

from eqsql import eq


def query_task(eq_work_type: int, query_timeout: float = 120.0,
               retry_threshold: int = 0, log_level=logging.WARN):
    eq.init(retry_threshold, log_level)
    try:
        eq.logger.debug('swift out_get')
        # result is a msg map
        msg_map = eq.query_task(eq_work_type, timeout=query_timeout)
        items = [msg_map['type'], msg_map['payload']]
        if msg_map['type'] == 'work':
            items.append(str(msg_map['eq_task_id']))
        # result_str should be returned via swift's python persist
        eq.logger.debug('swift out_get done')
        return '|'.join(items)
    except Exception:
        eq.logger.error(f'tasks.query_task error {traceback.format_exc()}')
        # result_str returned via swift's python persist
        return eq.ABORT_JSON_MSG
    finally:
        eq.close()


def report_task(eq_task_id: int, eq_work_type: int, result_payload: str,
                retry_threshold: int = 0, log_level=logging.WARN):
    eq.init(retry_threshold, log_level)
    try:
        # TODO this returns a ResultStatus, add FAILURE handling
        eq.report_task(eq_task_id, eq_work_type, result_payload)
    except Exception:
        eq.logger.error(f'tasks.report_task error {traceback.format_exc()}')
    finally:
        eq.close()


_q = mp.Queue(1)
_go = True


def query_tasks_n(batch_size: int, threshold: int, work_type: int, retry_threshold: int, q: mp.Queue):
    running_task_ids = []
    wait = 0.25
    while _go:
        eq.init(retry_threshold)
        try:
            running_task_ids, tasks = eq.query_more_tasks(work_type, running_task_ids,
                                                          batch_size=batch_size, threshold=threshold,
                                                          timeout=10)
        finally:
            eq.close()

        n_tasks = len(tasks)
        print("TASKS: ", tasks, flush=True)
        if n_tasks > 0:
            wait = 0.25
            if tasks[-1]['type'] == 'status':
                # Intention is that the stop / abort task
                # is pushed by itself.
                if n_tasks > 1:
                    q.put(tasks[:-1])
                    q.put([tasks[-1]])
                else:
                    q.put([tasks[0]])
            else:
                q.put(tasks)
        else:
            time.sleep(wait)
            if wait < 20:
                wait += 0.25


def init_task_querier(batch_size: int, threshold: int, work_type: int, retry_threshold: int = 0):
    # wait_info = WaitInfo
    t = threading.Thread(target=query_tasks_n, args=(batch_size, threshold, work_type,
                         retry_threshold, _q))
    t.start()


def get_tasks_n(msg_delimiter: str = '|', list_delimiter: str = ';'):
    global _q
    msg_maps = _q.get(True)
    msgs = []
    for msg_map in msg_maps:
        items = [msg_map['type'], msg_map['payload']]
        if msg_map['type'] == 'work':
            items.append(str(msg_map['eq_task_id']))
        msgs.append(msg_delimiter.join(items))

    return list_delimiter.join(msgs)


def stop_task_querier():
    global _go
    _go = False
