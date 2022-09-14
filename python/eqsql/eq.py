
# EQ-SQL eq.py
from asyncio import Task
from random import random
import threading
import traceback
import logging
import time
import json
from datetime import datetime, timezone
from enum import IntEnum
from typing import Tuple, Dict

from . import db_tools
from .db_tools import Q


class ResultStatus(IntEnum):
    SUCCESS = 0
    FAILURE = 1


EQ_ABORT = 'EQ_ABORT'
EQ_TIMEOUT = 'EQ_TIMEOUT'
EQ_STOP = 'EQ_STOP'


class TaskStatus(IntEnum):
    QUEUED = 0
    RUNNING = 1
    COMPLETE = 2
    CANCELED = 3


ABORT_JSON_MSG = json.dumps({'type': 'status', 'payload': EQ_ABORT})

p = None
aborted = False
wait_info = None
# The psycopg2 handle:
DB = None
logger = None


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


class Future:

    def __init__(self, eq_task_id: int, tag: str = None):
        self.eq_task_id = eq_task_id
        self.tag = tag

    def result(self, delay: float = 0.5, timeout: float = 2.0) -> Tuple:
        status_result = query_result(self.eq_task_id, delay, timeout=timeout)
        return status_result

    def status(self):
        return query_status(self.eq_task_id)


def init(retry_threshold=0, log_level=logging.WARN):
    """Initializes the eq module by connecting to the DB,
    and setting up logging.

    Args:
        retry_threshold: if a DB connection cannot be established
            (e.g, there are currently too many connections),
            then retry this many times to establish a connection. There
            will be random few second delay betwen each retry.
        log_level: the logging threshold level.
    """
    global logger
    logger = db_tools.setup_log(__name__, log_level)

    global DB
    if DB is not None:
        return

    retries = 0
    while True:
        try:
            DB = db_tools.setup_db(log_level=log_level, envs=True)
            DB.connect()
            break
        except db_tools.ConnectionException as e:
            retries += 1
            if retries > retry_threshold:
                raise(e)
            time.sleep(random() * 4)

    return DB


def close():
    """Closes the DB connection. eq.init() is required to re-initilaize the connection
    before calling any other functions.
    """
    global DB
    if DB is not None:
        DB.close()
        DB = None


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


def pop_out_queue(cur, eq_type: int, delay, timeout) -> Tuple:
    """Pops the highest priority task of the specified work type off
    of the db out queue.

    This call repeatedly polls for a task of the specified type. The polling
    interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_type: the type of the work to pop from the queue
        delay: the initial polling delay value
        timeout: the duration after which this call will timeout
            and return. If timeout is None, there is no limit to
            the wait time.

    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be the popped eq task id. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    sql_pop = _sql_pop_out_q(eq_type)
    res = _queue_pop(cur, sql_pop, delay, timeout)
    logger.debug(f'pop_out_queue: {str(res)}')
    return res


def pop_in_queue(cur, eq_task_id: int, delay, timeout) -> Tuple:
    """Pops the specified task off of the db in queue.

    This call repeatedly polls for a task with specified id. The polling
    interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_task_id: id of the task to pop
        delay: the initial polling delay value
        timeout: the duration after which this call will timeout
            and return. If timeout is None, there is no limit to
            the wait time.
    Returns: A two element tuple where the first elements is one of
        ResultStatus.SUCCESS or ResultStatus.FAILURE. On success the
        second element will be eq task id. On failure, the second
        element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
        cause of the failure.
    """
    sql_pop = _sql_pop_in_q(eq_task_id)
    res = _queue_pop(cur, sql_pop, delay, timeout)
    logger.debug(f'pop_in_queue: {str(res)}')
    return res


def _queue_pop(cur, sql_pop: str, delay: float, timeout: float) -> Tuple[ResultStatus, str]:
    """Performs the actual queue pop as defined the sql string.

    This call repeatedly attempts the pop operation by executing sql until
    the operation completes or the timeout duration has passed. The polling
    interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        cur: the db cursor to execute the sql with
        sql_pop: the sql query that defines the pop operation
        delay: the initial polling delay value
        timeout: the duration after which this call will timeout
            and return. If timeout is None, there is no limit to
            the wait time.

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
            cur.execute(sql_pop)
            rs = cur.fetchone()
            if rs is not None:
                break  # got good data
            if timeout is not None:
                if time.time() - start > timeout:
                    return (ResultStatus.FAILURE, EQ_TIMEOUT)
            time.sleep(delay)
            if delay < 30:
                delay += 0.25
    except Exception as e:
        logger.error(f'queue_pop error: {e}')
        logger.error(f'queue_pop error {traceback.format_exc()}')
        raise e

    return (ResultStatus.SUCCESS, rs[1])


def push_out_queue(cur, eq_task_id, eq_type, priority=0) -> ResultStatus:
    """Pushes the specified task onto the output queue with
    the specified priority.

    Args:
        eq_task_id: the id of the task
        eq_type: the type of the task
        priority: the priority of the task
    Returns:
        ResultStatus.SUCCESS if the task was successfully pushed
        onto the output queue, otherwise ResultStatus.FAILURE.
    """
    try:
        # queue_push("emews_queue_OUT", eq_type, eq_task_id, priority)
        insert_cmd = DB.format_insert('emews_queue_out', ["eq_task_type",
                                      "eq_task_id", "eq_priority"], [eq_type, eq_task_id, priority])
        cur.execute(insert_cmd)
        update_cmd = DB.format_update('eq_tasks', ['eq_status'], [TaskStatus.QUEUED.value],
                                      where=f'eq_task_id={eq_task_id}')
        cur.execute(update_cmd)

        return ResultStatus.SUCCESS

    except Exception as e:
        logger.error(f'push_out_queue error: {e}')
        logger.error(f'push_out_queue error {traceback.format_exc()}')
        raise(e)


def push_in_queue(cur, eq_task_id, eq_type) -> ResultStatus:
    """Pushes the specified task onto the input queue.

    Args:
        eq_task_id: the id of the task
        eq_type: the type of the task
    Returns:
        ResultStatus.SUCCESS if the task was successfully pushed
        onto the input queue, otherwise ResultStatus.FAILURE.
    """
    try:
        cmd = DB.format_insert('emews_queue_IN', ["eq_task_type", "eq_task_id"],
                               [eq_type, eq_task_id])
        cur.execute(cmd)
        return ResultStatus.SUCCESS
    except Exception as e:
        logger.error(f'push_in_queue error: {e}')
        logger.error(f'push_in_queue error {traceback.format_exc()}')
        return ResultStatus.FAILURE


def _insert_task(cur, exp_id: str, eq_type: int, payload: str) -> Tuple:
    """Inserts the specified payload to the database, creating
    a task entry for it and returning its assigned task id

    Args:
        exp_id: the id of the experiment that this task is part of
        eq_type: the work type of this task
        payload: the task payload

    Returns:
        A tuple whose first element is the ResultStatus of the insert, and
        whose second element is the task id assigned to this task if the insert
        was successfull, otherwise raises an exception.
    """
    try:
        cur.execute("select nextval('emews_id_generator');")
        rs = cur.fetchone()
        eq_task_id = rs[0]
        ts = datetime.now(timezone.utc).astimezone().isoformat()
        insert_cmd = DB.format_insert("eq_tasks", ["eq_task_id", "eq_task_type", "json_out", "time_created"],
                                      [eq_task_id, eq_type, Q(payload), Q(ts)])
        cur.execute(insert_cmd)
        insert_cmd = DB.format_insert("eq_exp_id_tasks", ["exp_id", "eq_task_id"],
                                      [Q(exp_id), eq_task_id])
        cur.execute(insert_cmd)
    except Exception as e:
        logger.error(f'insert_task error: {e}')
        logger.error(f'insert_task error {traceback.format_exc()}')
        raise(e)

    return (ResultStatus.SUCCESS, eq_task_id)


def select_task_payload(cur, eq_task_id: int) -> Tuple[ResultStatus, str]:
    """Selects the 'json_out' payload associated with the specified task id in
    the eq_tasks table, setting the start time of the task to
    the current time.

    Args:
        eq_task_id: the id of the task to get the json_out for

    Returns:
        A tuple containing the ResultStatus as its first element,
        and if successful the json_out payload
        for the specified task id as its second, otherwise the second element will
        be EQ_ABORT.
    """
    try:
        cmd = DB.format_select("eq_tasks", "json_out", f'eq_task_id={eq_task_id}')
        cur.execute(cmd)
        rs = cur.fetchone()
        ts = datetime.now(timezone.utc).astimezone().isoformat()
        cmd = DB.format_update("eq_tasks", ['eq_status', 'time_start'],
                               [TaskStatus.RUNNING.value, Q(ts)], where=f'eq_task_id={eq_task_id}')
        cur.execute(cmd)
        result = rs[0]
        return (ResultStatus.SUCCESS, result)
    except Exception as e:
        logger.error(f'select_task_payload error: {e}')
        logger.error(f'select_task_payload error {traceback.format_exc()}')
        raise(e)


def select_task_result(cur, eq_task_id: int) -> Tuple[ResultStatus, str]:
    """Selects the result ('json_in') payload associated with the specified task id in
    the eq_tasks table.

    Args:
        eq_task_id: the id of the task to get the json_in for

    Returns:
        A tuple containing the ResultStatus, and if successful the result payload
        for the specified task id, otherwise EQ_ABORT.
    """
    try:
        select_sql = DB.format_select("eq_tasks", "json_in", f'eq_task_id={eq_task_id}')
        cur.execute(select_sql)
        rs = cur.fetchone()
        result = rs[0]
    except Exception as e:
        logger.error(f'select_task_result error: {e}')
        logger.error(f'select_task_result error {traceback.format_exc()}')
        raise(e)

    return (ResultStatus.SUCCESS, result)


def update_task(cur, eq_task_id: int, payload: str) -> ResultStatus:
    """Updates the specified task in the eq_tasks table with the specified
    result ('json_in') payload. This also updates the 'time_stop'
    to the time when the update occurred.

    Args:
        eq_task_id: the id of the task to update
        payload: the payload to update the task with
    Returns:
        ResultStatus.SUCCESS if the task was successfully updated, otherwise
        ResultStatus.FAILURE.
    """
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    try:
        cmd = DB.formate_update("eq_tasks", ["eq_status", "json_in", 'time_stop'], [Q(payload),
                                TaskStatus.COMPLETE.value, Q(ts)], where=f'eq_task_id={eq_task_id}')
        cur.execute(cmd)
        return ResultStatus.SUCCESS
    except Exception as e:
        logger.error(f'update_task error: {e}')
        logger.error(f'update_task error {traceback.format_exc()}')
        return ResultStatus.FAILURE


def stop_worker_pool(eq_type: int) -> ResultStatus:
    """Stops any workers pools associated with the specified work type by
    pusing EQ_STOP into the queue.

    Args:
        eq_type: the work type for the pools to stop
    Returns:
        ResultStatus.SUCCESS if the stop message was successfully pushed, otherwise
        ResultStatus.FAILURE.
    """

    try:
        with DB.conn:
            with DB.conn.cursor() as cur:
                cur.execute("select nextval('emews_id_generator');")
                rs = cur.fetchone()
                eq_task_id = rs[0]
                cmd = DB.format_insert("eq_tasks", ["eq_task_id", "eq_task_type", "json_out"],
                                       [eq_task_id, eq_type, Q("EQ_STOP")])
                cur.execute(cmd)
                result_status = push_out_queue(cur, eq_task_id, eq_type, priority=-1)
                return result_status
    except Exception as e:
        logger.error(f'stop_worker_pool error: {e}')
        logger.error(f'stop_worker_pool error {traceback.format_exc()}')
        return ResultStatus.FAILURE


def query_task(eq_type: int, delay: float = 0.5, timeout: float = 2.0) -> Dict:
    """Queries for the highest priority task of the specified type.

    The query repeatedly polls for a task. The polling interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_type: the type of the task to query for
        delay: the initial polling delay value
        timeout: the duration after which the query will timeout. If timeout is None, there is no limit to
            the wait time.

    Returns:
        A dictionary formatted message. If the query results in a
        status update, the dictionary will have the following format:
        {'type': 'status', 'payload': P} where P is one of 'EQ_STOP',
        'EQ_ABORT', or 'EQ_TIMEOUT'. If the query finds work to be done
        then the dictionary will be:  {'type': 'work', 'eq_task_id': eq_task_id,
        'payload': P} where P is the parameters for the work to be done.
    """
    try:
        with DB.conn:
            with DB.conn.cursor() as cur:
                status, result = pop_out_queue(cur, eq_type, delay, timeout)
                logger.info(f'MSG: {status} {result}')
                if status == ResultStatus.SUCCESS:
                    eq_task_id = result
                    status, payload = select_task_payload(cur, eq_task_id)
                    if status == ResultStatus.SUCCESS:
                        if payload == EQ_STOP:
                            return {'type': 'status', 'payload': EQ_STOP}
                        else:
                            return {'type': 'work', 'eq_task_id': eq_task_id, 'payload': payload}
                else:
                    # timed out
                    return {'type': 'status', 'payload': result}
    except Exception:
        return {'type': 'status', 'payload': EQ_ABORT}


def submit_task(exp_id: str, eq_type: int, payload: str, priority: int = 0, tag: str = None) -> Future:
    """Submits work of the specified type and priority with the specified
    payload, returning the task id assigned to that task.

    Args:
        exp_id: the id of the experiment of which the work is part.
        eq_type: the type of work
        payload: the work payload
        priority: the priority of this work

    Returns:
        A tuple whose first element is the ResultStatus of the submission, and
        whose second element is the task id assigned to this task if the submission
        was successfull, otherwise -1.
    """
    try:
        with DB.conn:
            with DB.conn.cursor() as cur:
                _, eq_task_id = _insert_task(cur, exp_id, eq_type, payload)
                status = push_out_queue(cur, eq_task_id, eq_type, priority)

                return (status, Future(eq_task_id, tag))
    except Exception:
        return (ResultStatus.FAILURE, None)


def report_task(eq_task_id: int, eq_type: int, result: str) -> ResultStatus:
    """Reports the result of the specified task of the specified type

    Args:
        eq_task_id: the id of the task whose results are being reported.
        eq_type: the type of the task whose results are being reported.
        result: the result of the task.
    Returns:
        ResultStatus.SUCCESS if the task was successfully reported, otherwise
        ResultStatus.FAILURE.
    """
    with DB.conn:
        with DB.conn.cursor() as cur:
            # update_task doesn't throw an exception, so we won't
            # roll this back as we don't want to loose the result
            # if push_in_queue fails.
            result_status = update_task(cur, eq_task_id, result)
            if result_status == ResultStatus.SUCCESS:
                return push_in_queue(cur, eq_task_id, eq_type)
            else:
                return result_status


def query_status(eq_task_id: int) -> TaskStatus:
    try:
        with DB.conn:
            with DB.conn.cursor() as cur:
                cur.execute(f'select eq_status from eq_tasks where eq_task_id = {eq_task_id}')
                rs = cur.fetchone()
                status = rs[0]
    except Exception as e:
        logger.error(f'query_status error: {e}')
        logger.error(f'query_status error: {traceback.format_exc()}')
        return None

    return TaskStatus(status)


def query_result(eq_task_id: int, delay: float = 0.5, timeout: float = 2.0) -> Tuple:
    """Queries for the result of the specified task.

    The query repeatedly polls for a result. The polling interval is specified by
    the delay such that the first interval is defined by the initial delay value
    which is increased exponentionally after the first poll. The polling will
    timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        eq_task_id: the id of the task to query
        delay: the initial polling delay value
        timeout: the duration after which the query will timeout. If timeout is None, there is no limit to
            the wait time.

    Returns:
        A tuple whose first element indicates the status of the query:
        ResultStatus.SUCCESS or ResultStatus.FAILURE, and whose second element
        is either the result of the task, or in the case of failure the reason
        for the failure (EQ_TIMEOUT, or EQ_ABORT)
    """
    with DB.conn:
        with DB.conn.cursor() as cur:
            msg = pop_in_queue(cur, eq_task_id, delay, timeout)
            if msg[0] != ResultStatus.SUCCESS:
                return msg

        try:
            return select_task_result(cur, eq_task_id)
        except Exception:
            return (ResultStatus.FAILURE, EQ_ABORT)
