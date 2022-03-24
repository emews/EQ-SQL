
# EQ-SQL eq.py

import random
import sys
import threading
import traceback
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
EQ_FINAL = 'EQ_FINAL'

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


def init():
    global DB
    if DB is not None:
        return
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


def sql_pop_out_q(eq_type):
    """
    Generate code for a queue pop from emews_queue_out
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


def sql_pop_in_q(eq_task_id):
    """
    Generate code for a queue pop from emewws_queue_in
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


def pop_out_queue(eq_type: int, delay, timeout) -> Tuple[ResultStatus, str]:
    """
    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the popped data. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    sql_pop = sql_pop_out_q(eq_type)
    res = queue_pop(sql_pop, delay, timeout)
    print(f'pop_out_queue: {str(res)}', flush=True)
    return res


def pop_in_queue(eq_task_id: int, delay, timeout):
    """
    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the popped data. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    sql_pop = sql_pop_in_q(eq_task_id)
    res = queue_pop(sql_pop, delay, timeout)
    print(f'pop_in_queue: {str(res)}', flush=True)
    return res


def queue_pop(sql_pop: str, delay, timeout) -> Tuple[ResultStatus, str]:
    """
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
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print('{} ...\\n{}'.format(e, ''.joins(s)), flush=True)
        return (ResultStatus.FAILURE, EQ_ABORT)

    return (ResultStatus.SUCCESS, rs[1])


# def queue_push(table, eq_type, eq_task_id, priority):
#     DB.insert(table, ["eq_type",  "eq_task_id", "eq_priority"],
#                      [ eq_type, eq_task_id, priority])


def DB_submit(exp_id, eq_type, payload):
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


def DB_json_out(eq_task_id):
    """ return the json_out for the int eq_task_id """
    global DB
    DB.select("eq_tasks", "json_out", f'eq_task_id={eq_task_id}')
    rs = DB.get()
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    DB.update("eq_tasks", ['time_start'], [Q(ts)], where=f'eq_task_id={eq_task_id}')
    result = rs[0]
    return result


def DB_json_in(eq_task_id):
    """ return the json_in for the int eq_task_id """
    global DB
    DB.select("eq_tasks", "json_in", f'eq_task_id={eq_task_id}')
    rs = DB.get()
    result = rs[0]
    return result


def DB_result(eq_task_id, payload):
    global DB
    print("DB_result:", flush=True)
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    DB.update("eq_tasks", ["json_in", 'time_stop'], [Q(payload), Q(ts)],
              where=f'eq_task_id={eq_task_id}')


def DB_final(eq_type: int):
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_task_id = rs[0]
    DB.insert("eq_tasks", ["eq_task_id", "eq_task_type", "json_out"],
              [eq_task_id, eq_type, Q("EQ_FINAL")])
    OUT_put(eq_type, eq_task_id)
    return eq_task_id


def OUT_put(eq_type, eq_task_id, priority=0):
    """"""
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


def IN_put(eq_type, eq_task_id):
    try:
        DB.insert('emews_queue_IN', ["eq_task_type", "eq_task_id"],
                  [eq_type, eq_task_id])
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()


def OUT_get(eq_type, delay=0.5, timeout=2.0):
    """
    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the queued data of the specified type.
        On failure, the second element will be one of EQ_ABORT or EQ_TIMEOUT
        depending on the cause of the failure.
    """
    print("OUT_get():", flush=True)
    pop_result = pop_out_queue(eq_type, delay, timeout)
    if pop_result[0] != ResultStatus.SUCCESS:
        print(f'eq.py:OUT_get(eq_task_type={eq_type}): {pop_result[1]}!', flush=True)

    return pop_result


def IN_get(eq_task_id, delay=0.5, timeout=2.0):
    """
    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the queued data for the specified task id.
        On failure, the second element will be one of EQ_ABORT or EQ_TIMEOUT
        depending on the cause of the failure.
    """

    pop_result = pop_in_queue(eq_task_id, delay, timeout)
    if pop_result[0] != ResultStatus.SUCCESS:
        print(f'eq.py:IN_get(eq_task_id={eq_task_id}): {pop_result[1]}!', flush=True)

    return pop_result


def done(msg):
    if msg == "EQ_FINAL":
        return True
    if msg == "EQ_ABORT" or msg == "EQ_TIMEOUT":
        print(f"eq.done(): WARNING: {msg}")
        return True
    return False


def query_task(eq_type: int, timeout: float=2.0) -> Dict:
    """
    Queries the database for work of the specified type.

    Args:
        eq_type: the id of the work type
        timeout: how long to wait for a response before timing out
        and returning {'type': 'status', 'payload': EQ_TIMEOUT}

    Returns:
        A dictionary formatted message. If the query results in a
        status update, the dictionary will have the following format:
        {'type': 'status', 'payload': P} where P is one of 'EQ_FINAL',
        'EQ_ABORT', or 'EQ_TIMEOUT'. If the query specifies work to be done
        then the dictionary will be:  {'type': 'work', 'eq_task_id': eq_task_id,
        'payload': P} where P is the parameters for the work to be done.

    """
    status, result = OUT_get(eq_type, timeout=timeout)
    print('MSG:', status, result, flush=True)
    if status == ResultStatus.SUCCESS:
        eq_task_id = result
        payload = DB_json_out(eq_task_id)
        if payload == EQ_FINAL:
            return {'type': 'status', 'payload': EQ_FINAL}
        else:
            return {'type': 'work', 'eq_task_id': eq_task_id, 'payload': payload}
        # return (eq_task_id, payload)
    else:
        return {'type': 'status', 'payload': result}
        # return (-1, result)


def sumbit_task(exp_id: str, eq_type: int, payload: str, priority: int=0) -> int:
    """Submits work of the specified type and priority with the specified
    payload, returning the task id for that work.

    Args:
        exp_id: the id of the experiment of which the work is part.
        eq_type: the type of work
        payload: the work payload
        priority: the priority of this work

    Returns:
        task_id: the task id for the work
    """
    eq_task_id = DB_submit(exp_id, eq_type, payload)
    OUT_put(eq_type, eq_task_id, priority)
    return eq_task_id


def report_task(eq_type: int, eq_task_id: int, result: str):
    """Reports the result of the specified task of the specified type"""
    DB_result(eq_task_id, result)
    IN_put(eq_type, eq_task_id)
