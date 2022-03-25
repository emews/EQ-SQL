
# EQ-SQL eq.py

import random
import sys
import threading
import traceback
import logging
import time
import json
from datetime import datetime, timezone
from enum import IntEnum
from typing import Tuple, Dict

import db_tools
from db_tools import Q


class ResultStatus(IntEnum):
    SUCCESS = 0
    FAILURE = 1


EQ_ABORT = 'EQ_ABORT'
EQ_TIMEOUT = 'EQ_TIMEOUT'
EQ_STOP = 'EQ_STOP'

ABORT_JSON_MSG = json.dumps({'type': 'status', 'payload': EQ_ABORT})

p = None
aborted = False
wait_info = None
# The psycopg2 handle:
DB = None


class WaitInfo:

    def __init__(self):
        self.wait = 4

    def getWait(self):
        if self.wait < 60:
            self.wait += 1
        return self.wait


class ThreadRunner(threading.Thread):

    def __init__(self, runnable):
        threading.Thread.__init__(self)
        self.runnable = runnable
        self.exc = "Exited normally"

    def run(self):
        try:
            self.runnable.run()
        except BaseException:
            # tuple of type, value and traceback
            self.exc = traceback.format_exc()


def init(log_level=logging.WARN):
    """Initializes the eq module by connecting to the DB,
    and setting up logging.

    Args:
        log_level: the default logging level.
    """
    global DB
    if DB is not None:
        return
    # TODO: update to use log_level
    DB = db_tools.setup_db(envs=True, log=False)
    DB.connect()
    return DB


def validate():
    """ Connect to DB or die! """
    global DB
    # This code has no effect except to validate the connection:
    try:
        DB.execute("select * from emews_id_generator;")
        DB.get()
    except Exception:
        print("ERROR: eq.validate() failed!")
        sys.stdout.flush()
        return None
    return "EQ-SQL:OK"


def _sql_pop_out_q(eq_type) -> str:
    """
    Generate sql for a queue pop from emews_queue_out
    From:
    https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    Can only select 1 column from the subquery,
    but we return * from the deleted row.
    See workflow.sql for the returned queue row
    """
    code = """
    DELETE FROM emews_queue_OUT
    WHERE  eq_task_id = (
    SELECT eq_task_id
    FROM emews_queue_OUT
    WHERE eq_task_type = {}
    ORDER BY eq_priority DESC, eq_task_id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    """.format(eq_type)
    return code


def _sql_pop_in_q(eq_task_id) -> str:
    """
    Generate sql for a queue pop from emewws_queue_in
    From:
    https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    Can only select 1 column from the subquery,
    but we return * from the deleted row.
    See workflow.sql for the returned queue row
    """
    code = """
    DELETE FROM emews_queue_IN
    WHERE  eq_task_id = (
    SELECT eq_task_id
    FROM emews_queue_IN
    WHERE eq_task_id = {}
    ORDER BY eq_task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    """.format(eq_task_id)
    return code


def pop_out_queue(eq_type: int, delay, timeout) -> Tuple[ResultStatus, int]:
    """Pops the highest priority task of the secified work type off
    of the out db queue.

    This call repeatedly polls for a task of the specified type. The polling
    interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_type: the type of the work to pop from the queue
        delay: the initial polling delay value
        timeout: the duration after which this call will timeout
            and return.

    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the popped eq task id. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    sql_pop = _sql_pop_out_q(eq_type)
    res = _queue_pop(sql_pop, delay, timeout)
    print(f'pop_out_queue: {str(res)}', flush=True)
    return res


def pop_in_queue(eq_task_id: int, delay, timeout):
    """Pops the specified task off of the in db queue.

    This call repeatedly polls for a task with specified id. The polling
    interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_task_id: id of the task to pop
        delay: the initial polling delay value
        timeout: the duration after which this call will timeout
            and return.
    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be eq task id. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    sql_pop = _sql_pop_in_q(eq_task_id)
    res = _queue_pop(sql_pop, delay, timeout)
    print(f'pop_in_queue: {str(res)}', flush=True)
    return res


def _queue_pop(sql_pop: str, delay: float, timeout: float) -> Tuple[ResultStatus, str]:
    """Performs the actual queue pop as defined the sql string.

    This call repeatedly attempts the pop operation by executing sql until
    the operation completes or the timeout duration has passed. The polling
    interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        sql_pop: the sql query that defines the pop operation
        delay: the initial polling delay value
        timeout: the duration after which this call will timeout
            and return.

    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the popped eq_task_id. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    global DB
    start = time.time()
    try:
        while True:
            DB.execute(sql_pop)
            rs = DB.get()
            if rs is not None:
                break  # got good data
            if time.time() - start > timeout:
                return (ResultStatus.FAILURE, EQ_TIMEOUT)
                break  # timeout
            delay = delay * random.random() * 2
            time.sleep(delay)
            delay = delay * 2
    except Exception as e:
        # raise(e)
        print(e)
        print(traceback.format_exc())
        # info = sys.exc_info()
        # s = traceback.format_tb(info[2])
        # print(s)
        # print('{} ...\\n{}'.format(e, ''.joins(s)), flush=True)
        return (ResultStatus.FAILURE, EQ_ABORT)

    return (ResultStatus.SUCCESS, rs[1])


def push_out_queue(eq_task_id, eq_type, priority=0):
    """Pushes the specified task onto the output queue with
    the specified priority.

    Args:
        eq_task_id: the id of the task
        eq_type: the type of the task
        priority: the priority of the task
    """
    try:
        # queue_push("emews_queue_OUT", eq_type, eq_task_id, priority)
        DB.insert('emews_queue_OUT', ["eq_task_type", "eq_task_id", "eq_priority"],
                  [eq_type, eq_task_id, priority])

    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()
    return eq_task_id


def push_in_queue(eq_task_id, eq_type):
    """Pushes the specified task onto the input queue.

    Args:
        eq_task_id: the id of the task
        eq_type: the type of the task
    """
    try:
        DB.insert('emews_queue_IN', ["eq_task_type", "eq_task_id"],
                  [eq_type, eq_task_id])
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()


def insert_task(exp_id: str, eq_type: int, payload: str) -> int:
    """Inserts the specified payload to the database, creating
    a task entry for it and returning its assigned task id

    Args:
        exp_id: the id of the experiment that this task is part of
        eq_type: the work type of this task
        payload: the task payload

    Returns:
        The task id assigned to this task.
    """
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_task_id = rs[0]
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    DB.insert("eq_tasks", ["eq_task_id", "eq_task_type", "json_out", "time_created"],
              [eq_task_id, eq_type, Q(payload), Q(ts)])
    DB.insert("eq_exp_id_tasks", ["exp_id", "eq_task_id"],
              [Q(exp_id), eq_task_id])
    return eq_task_id


def select_task_payload(eq_task_id: int) -> str:
    """Selects the 'json_out' payload associated with the specified task id in
    the eq_tasks table, setting the start time of the task to
    the current time.

    Args:
        eq_task_id: the id of the task to get the json_out for

    Returns:
        The json_out payload for the specified task id.
    """
    global DB
    DB.select("eq_tasks", "json_out", f'eq_task_id={eq_task_id}')
    rs = DB.get()
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    DB.update("eq_tasks", ['time_start'], [Q(ts)], where=f'eq_task_id={eq_task_id}')
    result = rs[0]
    return result


def select_task_result(eq_task_id: int) -> str:
    """Selects the result ('json_in') payload associated with the specified task id in
    the eq_tasks table.

    Args:
        eq_task_id: the id of the task to get the json_in for

    Returns:
        The result payload for the specified task id.
    """
    global DB
    DB.select("eq_tasks", "json_in", f'eq_task_id={eq_task_id}')
    rs = DB.get()
    result = rs[0]
    return result


def update_task(eq_task_id: int, payload: str):
    """Updates the specified task with the specified result ('json_in') payload

    Args:
        eq_task_id: the id of the task to update
        payload: the payload to update the task with
    """
    global DB
    print("DB_result:", flush=True)
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    DB.update("eq_tasks", ["json_in", 'time_stop'], [Q(payload), Q(ts)],
              where=f'eq_task_id={eq_task_id}')


def stop_worker_pool(eq_type: int):
    """Stops any workers pools associated with the specified work type by
    pusing EQ_STOP into the queue.

    Args:
        eq_type: the work type for the pools to stop
    """
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_task_id = rs[0]
    DB.insert("eq_tasks", ["eq_task_id", "eq_task_type", "json_out"],
              [eq_task_id, eq_type, Q("EQ_STOP")])
    push_out_queue(eq_task_id, eq_type, priority=-1)
    return eq_task_id


def query_task(eq_type: int, delay: float = 0.5, timeout: float = 2.0) -> Dict:
    """Queries for the highest priority task of the specified type.

    The query repeatedly polls for a task. The polling interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_type: the type task to query for
        delay: the initial polling delay value
        timeout: the duration after which the query will timeout

    Returns:
        A dictionary formatted message. If the query results in a
        status update, the dictionary will have the following format:
        {'type': 'status', 'payload': P} where P is one of 'EQ_STOP',
        'EQ_ABORT', or 'EQ_TIMEOUT'. If the query finds work to be done
        then the dictionary will be:  {'type': 'work', 'eq_task_id': eq_task_id,
        'payload': P} where P is the parameters for the work to be done.

    """
    status, result = pop_out_queue(eq_type, delay, timeout)
    print('MSG:', status, result, flush=True)
    if status == ResultStatus.SUCCESS:
        eq_task_id = result
        payload = select_task_payload(eq_task_id)
        if payload == EQ_STOP:
            return {'type': 'status', 'payload': EQ_STOP}
        else:
            return {'type': 'work', 'eq_task_id': eq_task_id, 'payload': payload}
        # return (eq_task_id, payload)
    else:
        return {'type': 'status', 'payload': result}
        # return (-1, result)


def submit_task(exp_id: str, eq_type: int, payload: str, priority: int = 0) -> int:
    """Submits work of the specified type and priority with the specified
    payload, returning the task id assigned to that task.

    Args:
        exp_id: the id of the experiment of which the work is part.
        eq_type: the type of work
        payload: the work payload
        priority: the priority of this work

    Returns:
        task_id: the task id for the work
    """
    eq_task_id = insert_task(exp_id, eq_type, payload)
    push_out_queue(eq_task_id, eq_type, priority)
    return eq_task_id


def report_task(eq_task_id: int, eq_type: int, result: str):
    """Reports the result of the specified task of the specified type"""
    update_task(eq_task_id, result)
    push_in_queue(eq_task_id, eq_type)


def query_result(eq_task_id: int, delay: float = 0.5, timeout: float = 2.0) -> Tuple:
    """Queries for the result of the specified task.

    The query repeatedly polls for a result. The polling interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_task_id: the id of the task to query
        delay: the initial polling delay value
        timeout: the duration after which the query will timeout

    Returns:
        A tuple whose first element indicates the status of the query:
        ResultStatus.SUCCESS or ResultStatus.FAILURE, and whose second element
        is either result of the task, or in the case of failure the reason
        for the failure (EQ_TIMEOUT, or EQ_ABORT)

    """
    msg = pop_in_queue(eq_task_id, delay, timeout)
    if msg[0] != ResultStatus.SUCCESS:
        return msg

    result = select_task_result(eq_task_id)
    return (ResultStatus.SUCCESS, result)
