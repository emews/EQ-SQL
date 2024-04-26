"""Local task queue implementations. A local task queue is one which
communicates directly with the database, rather than through some intermediary
service (e.g., Globus Computer or a RESTful API)"""


from random import random
import traceback
import logging
import time
from datetime import datetime, timezone
from typing import Iterable, Tuple, Dict, List, Generator, Union

from eqsql import db_tools
from eqsql.db_tools import WorkflowSQL
from eqsql.task_queues.core import ResultStatus, TaskStatus, TimeoutError
from eqsql.task_queues.core import EQ_ABORT, EQ_STOP, EQ_TIMEOUT
from eqsql.task_queues.core import Future, TaskQueue


_log_id = 1


class LocalTaskQueue:

    def __init__(self, db: WorkflowSQL, logger: logging.Logger):
        """Creates an LocalTaskQueue task queue connected to the specified database, logging to
        the specified logger. LocalTaskQueue tasks queues should be created with
        :py:func:`init_task_queue`.

        Args:
            db: the database to submit and retrieve tasks from
            logger: the logger to use for logging
        """
        self.db = db
        self.logger = logger

    def close(self):
        """Closes the DB connection, and terminates this :py:class:`LocalTaskQueue` instance.
        """
        if self.db:
            self.db.close()
        self.db = None

    def _sql_pop_out_q(self, eq_type: int, n: int = 1) -> str:
        """
        Generates sql for a queue pop from emews_queue_out
        From:
        https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
        Can only select 1 column from the subquery,
        but we return * from the deleted row.
        See workflow.sql for the returned queue row

        Args:
            eq_type: the work type to pop.
            n: the maximum number of eq_task_ids the sql query
                should return.
        Returns:
            The sql query to pop from the out queue.
        """
        code = f"""
        DELETE FROM emews_queue_OUT
        WHERE  eq_task_id = any( array(
        SELECT eq_task_id
        FROM emews_queue_OUT
        WHERE eq_task_type = {eq_type}
        ORDER BY eq_priority DESC, eq_task_id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT {n}
        ))
        RETURNING *;
        """
        return code

    def _sql_pop_in_q(self, eq_task_id) -> str:
        """
        Format sql for a queue pop from emewws_queue_in
        From:
        https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
        Can only select 1 column from the subquery,
        but we return * from the deleted row.
        See workflow.sql for the returned queue row

        Args:
            The eq_task_id to pop

        Returns:
            The sql query to pop from the in queue.
        """
        code = f"""
        DELETE FROM emews_queue_IN
        WHERE  eq_task_id = (
        SELECT eq_task_id
        FROM emews_queue_IN
        WHERE eq_task_id = {eq_task_id}
        ORDER BY eq_task_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        )
        RETURNING *;
        """
        return code

    def pop_out_queue(self, cur, eq_type: int, n: int, delay: float,
                      timeout: float) -> Tuple[ResultStatus, Union[int, str]]:
        """Pops the highest priority task of the specified work type off
        of the db out queue.

        This call repeatedly polls for a task of the specified type. The polling
        interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is then incremented after each poll. The polling will
        timeout after the amount of time specified by the timout value is has elapsed.

        Args:
            eq_type: the type of the work to pop from the queue
            n: the maximum number of tasks to pop from the queue
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
        sql_pop = self._sql_pop_out_q(eq_type, n)
        # print(sql_pop)
        res = self._queue_pop(cur, sql_pop, delay, timeout)
        self.logger.debug(f'pop_out_queue sql:\n{sql_pop}')
        self.logger.debug(f'pop_out_queue: {res}')
        return res

    def pop_in_queue(self, cur, eq_task_id: int, delay: float, timeout: float) -> Tuple[ResultStatus, Union[int, str]]:
        """Pops the specified task off of the db in queue.

        This call repeatedly polls for a task with the specified id. The polling
        interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is then incremented after each poll. The polling will
        timeout after the amount of time specified by the timout value is has elapsed.

        Args:
            cur: the database cursor
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
        sql_pop = self._sql_pop_in_q(eq_task_id)
        res = self._queue_pop(cur, sql_pop, delay, timeout)
        self.logger.debug(f'pop_in_queue: {res}')
        return res

    def _queue_pop(self, cur, sql_pop: str, delay: float,
                   timeout: float) -> Tuple[ResultStatus, Union[List[int], str]]:
        """Performs the actual queue pop as defined the sql string.

        This call repeatedly attempts the pop operation by executing sql until
        the operation completes or the timeout duration has passed. The polling
        interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is then incremented after each poll. The polling will
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
            second element will be a list of the popped eq_task_ids.
            On failure, the second
            element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
            cause of the failure.
        """
        start = time.time()
        results = []
        try:
            while True:
                cur.execute(sql_pop)
                # returns task_type, task_id, priority
                results = [res[1] for res in cur.fetchall()]
                # print(f'Results: {results}')
                if len(results) > 0:
                    break  # got good data
                if timeout is not None:
                    if time.time() - start > timeout:
                        return (ResultStatus.FAILURE, EQ_TIMEOUT)
                time.sleep(delay)
                if delay < 30:
                    delay += 0.25
        except Exception as e:
            self.logger.error(f'queue_pop error {traceback.format_exc()}')
            raise e

        return (ResultStatus.SUCCESS, results)

    def push_out_queue(self, cur, eq_task_id: int, eq_type: int, priority: int = 0):
        """Pushes the specified task onto the output queue with
        the specified priority.

        Args:
            cur: the database cursor used to execute the sql push
            eq_task_id: the id of the task
            eq_type: the type of the task
            priority: the priority of the task
        """
        try:
            # queue_push("emews_queue_OUT", eq_type, eq_task_id, priority)
            insert_cmd = db_tools.format_insert('emews_queue_out', ["eq_task_type", "eq_task_id",
                                                "eq_priority"])
            cur.execute(insert_cmd, [eq_type, eq_task_id, priority])
            update_cmd = db_tools.format_update('eq_tasks', ['eq_status'], where='eq_task_id=%s')
            cur.execute(update_cmd, [TaskStatus.QUEUED.value, eq_task_id])

        except Exception as e:
            self.logger.error(f'push_out_queue error {traceback.format_exc()}')
            raise e

    def push_in_queue(self, cur, eq_task_id: int, eq_type: int) -> ResultStatus:
        """Pushes the specified task onto the input queue.

        Args:
            cur: the database cursor used to execute the sql push
            eq_task_id: the id of the task
            eq_type: the type of the task
        Returns:
            ResultStatus.SUCCESS if the task was successfully pushed
            onto the input queue, otherwise ResultStatus.FAILURE.
        """
        try:
            cmd = db_tools.format_insert('emews_queue_IN', ["eq_task_type", "eq_task_id"])
            cur.execute(cmd, [eq_type, eq_task_id])
            return ResultStatus.SUCCESS
        except Exception:
            self.logger.error(f'push_in_queue error {traceback.format_exc()}')
            return ResultStatus.FAILURE

    def _insert_task(self, cur, exp_id: str, eq_type: int, payload: str, priority: int) -> int:
        """Inserts the specified payload to the database, creating
        a task entry for it and returning its assigned task id

        Args:
            cur: the database cursor used to execute the insert
            exp_id: the id of the experiment that this task is part of
            eq_type: the work type of this task
            payload: the task payload

        Returns:
            The task id assigned to this task if the insert
            was successfull, otherwise raise an exception.
        """
        try:
            cur.execute("select nextval('emews_id_generator');")
            rs = cur.fetchone()
            eq_task_id = rs[0]
            ts = datetime.now(timezone.utc).astimezone().isoformat()
            insert_cmd = db_tools.format_insert("eq_tasks", ["eq_task_id", "eq_task_type",
                                                "json_out", "time_created", "eq_priority"])
            cur.execute(insert_cmd, [eq_task_id, eq_type, payload, ts, priority])
            insert_cmd = db_tools.format_insert("eq_exp_id_tasks", ["exp_id", "eq_task_id"])
            cur.execute(insert_cmd, [exp_id, eq_task_id])
        except Exception as e:
            self.logger.error(f'insert_task error {traceback.format_exc()}')
            raise e

        return eq_task_id

    def select_task_payload(self, cur, eq_task_ids: Iterable[int], worker_pool_id: str = 'default') -> List[Tuple[int, str]]:
        """Selects the ``json_out`` payload associated with the specified task ids in
        the ``eq_tasks`` table, setting the start time of the tasks to
        the current time, the status of the tasks to :py:class:`TaskStatus.RUNNING`, and the
        worker_pool to the specified worker_pool.

        Args:
            cur: the database cursor used to execute the sql select
            eq_task_ids: the ids of the tasks to get the json_out for
            worker_pool_id: the id of the worker pool asking for the payload

        Returns:
            If successful, a list of tuples containing an ``eq_task_id`` and
            the ``json_out`` payload, otherwise raise an exception.
        """

        placeholders = ', '.join(['%s'] * len(eq_task_ids))
        task_ids = list(eq_task_ids)
        try:
            query = f'select eq_task_id, json_out from eq_tasks where eq_task_id in ({placeholders}) ORDER BY eq_task_id ASC'
            cur.execute(query, task_ids)
            result = [(rs[0], rs[1]) for rs in cur.fetchall()]
            # cmd = db_tools.format_select("eq_tasks", "json_out", 'eq_task_id=%s')
            # cur.execute(cmd, (eq_task_id,))
            # rs = cur.fetchone()
            ts = datetime.now(timezone.utc).astimezone().isoformat()
            cmd = f'update eq_tasks set eq_status=%s, worker_pool=%s, time_start=%s where eq_task_id in ({placeholders})'
            cur.execute(cmd, [TaskStatus.RUNNING.value, worker_pool_id, ts] + task_ids)
            return result
        except Exception as e:
            self.logger.error(f'select_task_payload error {traceback.format_exc()}')
            raise e

    def select_task_result(self, cur, eq_task_id: int) -> str:
        """Selects the result (``json_in``) payload associated with the specified task id in
        the ``eq_tasks`` table.

        Args:
            eq_task_id: the id of the task to get the json_in for

        Returns:
            The result payload for the specified task id, if successfull, otherwise
            raise an exception.
        """
        try:
            select_sql = db_tools.format_select("eq_tasks", "json_in", 'eq_task_id=%s')
            cur.execute(select_sql, (eq_task_id,))
            rs = cur.fetchone()
            result = rs[0]
        except Exception as e:
            self.logger.error(f'select_task_result error {traceback.format_exc()}')
            raise e

        return result

    def _get(self, select_query: str, *args):
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    cur.execute(select_query, args)
                    return (ResultStatus.SUCCESS, cur.fetchall())

        except Exception:
            self.logger.error(f'_get error {traceback.format_exc()}')
            return (ResultStatus.FAILURE, [])

    def update_task(self, cur, eq_task_id: int, payload: str):
        """Updates the specified task in the ``eq_tasks`` table with the specified
        result payload (the ``json_in``). This also updates the ``time_stop``
        to the time when the update occurred.

        Args:
            eq_task_id: the id of the task to update
            payload: the payload to update the task with
        """
        ts = datetime.now(timezone.utc).astimezone().isoformat()
        try:
            cmd = db_tools.format_update("eq_tasks", ['json_in', 'eq_status', 'time_stop'],
                                         where='eq_task_id=%s')
            cur.execute(cmd, [payload, TaskStatus.COMPLETE.value, ts, eq_task_id])
        except Exception as e:
            self.logger.error(f'update_task error {traceback.format_exc()}')
            raise e

    def stop_worker_pool(self, eq_type: int) -> ResultStatus:
        """Stops any workers pools associated with the specified work type by
        pushing ``EQ_STOP`` into the queue.

        Args:
            eq_type: the work type for the pools to stop
        Returns:
            :py:class:`ResultStatus.SUCCESS` if the stop message was successfully pushed, otherwise
            :py:class:`ResultStatus.FAILURE`.
        """
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    cur.execute("select nextval('emews_id_generator');")
                    rs = cur.fetchone()
                    eq_task_id = rs[0]
                    cmd = db_tools.format_insert("eq_tasks", ["eq_task_id", "eq_task_type", "json_out"])
                    cur.execute(cmd, [eq_task_id, eq_type, EQ_STOP])
                    self.push_out_queue(cur, eq_task_id, eq_type, priority=-1)
                    return ResultStatus.SUCCESS
        except Exception:
            self.logger.error(f'stop_worker_pool error {traceback.format_exc()}')
            return ResultStatus.FAILURE

    def submit_task(self, exp_id: str, eq_type: int, payload: str, priority: int = 0,
                    tag: str = None) -> Tuple[ResultStatus, Union[Future, None]]:
        """Submits work of the specified type and priority with the specified
        payload, returning the :py:class:`status <ResultStatus>` and the :py:class:`Future` encapsulating the submission.

        Args:
            exp_id: the id of the experiment of which the work is part.
            eq_type: the type of work
            payload: the work payload
            priority: the priority of this work
            tag: an optional metadata tag for the task

        Returns:
            A tuple containing the status (:py:class:`ResultStatus.FAILURE` or :py:class:`ResultStatus.SUCCESS`) of the submission
            and if successful, a :py:class:`Future` representing the submitted task otherwise None.
        """
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    eq_task_id = self._insert_task(cur, exp_id, eq_type, payload, priority)
                    if tag is not None:
                        cmd = db_tools.format_insert('eq_task_tags', ['eq_task_id', 'tag'])
                        cur.execute(cmd, (eq_task_id, tag))
                    self.push_out_queue(cur, eq_task_id, eq_type, priority)
                    return (ResultStatus.SUCCESS, Future(self, eq_task_id, tag))
        except Exception:
            self.logger.error(f'submit_task error {traceback.format_exc()}')
            return (ResultStatus.FAILURE, None)

    def submit_tasks(self, exp_id: str, eq_type: int, payload: List[str], priority: int = 0,
                     tag: str = None) -> Tuple[ResultStatus, List[Future]]:
        """Submits work of the specified type and priority with the specified
        payloads, returning the :py:class:`status <ResultStatus>` and the :py:class:`futures <Future>`
        encapsulating the submission.
        This is essentially a convenience wrapper around :py:func:`~LocalTaskQueue.submit_task` for submitting
        a list of payloads.

        Args:
            exp_id: the id of the experiment of which the work is part.
            eq_type: the type of work
            payload: a list of the work payloads
            priority: the priority of this work
            tag: an optional metadata tag for the tasks

        Returns:
            A tuple containing the status (:py:class:`ResultStatus.FAILURE` or :py:class:`ResultStatus.SUCCESS`)
            of the submission and the list of :py:class:`futures <Future>` for the submitted tasks. If the submission fails,
            the list of :py:class:`futures <Future>` will contain the :py:class:`futures <Future>` that submitted sucessfully.
        """
        fts = []
        for task in payload:
            rs, ft = self.submit_task(exp_id, eq_type, task, priority, tag)
            if rs != ResultStatus.SUCCESS:
                break
            fts.append(ft)
        return (rs, fts)

    def query_more_tasks(self, eq_type: int, eq_task_ids: Iterable[int], batch_size: int, threshold: int = 1,
                         worker_pool: str = 'default', delay: float = 0.5, timeout: float = 2.0) -> Tuple[List[int], List[Dict]]:
        """Queries for tasks of the specified type, returning up to batch_size number of tasks. The
        exact number of task to return is batch_size - *X* where *X* is the number of currently running tasks
        from those in eq_task_ids. The intention here is that a worker pool may have a limited amount
        of capacity and should not get more tasks than that capacity. eq_task_ids keeps track of the number
        tasks the worker pool is working on and batch_size is the maximum amount of work (tasks)
        the worker pool wants to execute. When the difference between batch size and the number of
        running tasks is greater than the threshold then query for tasks.

        The query repeatedly polls for tasks. The polling
        interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is then incremented after each poll. The polling will
        timeout after the amount of time specified by the timout value is has elapsed.

        Args:
            eq_type: the type of the work to query for.
            eq_task_ids: the possibly running task ids used to determine the number of tasks to return.
            batch_size: the maximum amount of tasks to return
            threshold: the number of free "slots" (difference between running workers and batch_size)
                that must be available before tasks are queried for. This must be less than or equal to
                batch size.
            worker_pool: the id of the worker pool querying for the tasks
            delay: the initial polling delay value
            timeout: the duration after which the query will timeout. If timeout is None, there is no limit to
                the wait time.


        Returns:
            A two element Tuple where the first element is a List of the ids of the currently running tasks
            from those specified in eq_task_ids plus the ids of any new tasks, and the second element
            is a List of Dictionaries as returned by :py:func:`~LocalTaskQueue.query_task`.
        """
        if threshold < 1:
            raise ValueError(f'Invalid threshold: threshold must be greater than 0: threshold = {threshold}')

        if batch_size < 1:
            raise ValueError(f'Invalid batch_size: batch_size must be greater than 0: batch_size = {batch_size}')

        if threshold > batch_size:
            raise ValueError('Invalid threshold / batch_size: threshold of must be less than '
                             f'or equal to batch_size: threshold = {threshold}, batch_size = {batch_size}')

        running_tasks = []
        if len(eq_task_ids) > 0:
            statuses = self._query_status(eq_task_ids)
            if statuses is None:
                return ([], [{'type': 'status', 'payload': EQ_ABORT}])

            running_tasks = [eq_task_id for eq_task_id, _ in
                             filter(lambda item: item[1] == TaskStatus.RUNNING, statuses)]

        # print("batch size", batch_size, flush=True)
        n_query = batch_size - len(running_tasks)
        # print(f'n_query: {n_query}', flush=True)
        if n_query >= threshold:
            new_tasks = self.query_task(eq_type, n=n_query, worker_pool=worker_pool, delay=delay, timeout=timeout)
            if n_query == 1:
                if 'eq_task_id' in new_tasks:
                    return (running_tasks + [new_tasks['eq_task_id']], [new_tasks])
                else:
                    # stop or abort msg
                    return (running_tasks, [new_tasks])
            else:
                # can be single dictionary if abort or timeout
                if not isinstance(new_tasks, List):
                    new_tasks = [new_tasks]
                return (running_tasks + [task['eq_task_id'] for task in
                        filter(lambda task: 'eq_task_id' in task, new_tasks)],
                        new_tasks)
        else:
            return (running_tasks, [])

    def query_task(self, eq_type: int, n: int = 1, worker_pool: str = 'default', delay: float = 0.5, timeout: float = 2.0) -> Union[List[Dict], Dict]:
        """Queries for the highest priority task of the specified type.

        The query repeatedly polls for n number of tasks. The polling
        interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is then incremented after each poll. The polling will
        timeout after the amount of time specified by the timout value is has elapsed.

        Args:
            eq_type: the type of the task to query for
            n: the maximum number of tasks to query for
            worker_pool: the id of the worker pool query for the tasks
            delay: the initial polling delay value
            timeout: the duration after which the query will timeout. If timeout is None, there is no limit to
                the wait time.

        Returns:
            If ``n == 1``, a single dictionary will be returned, otherwise
            a List of dictionaries will be returned. In both cases, the
            dictionary(ies) contain the following. If the query results in a
            status update, the dictionary will have the following format:
            ``{'type': 'status', 'payload': P}`` where ``P`` is one of ``'EQ_STOP'``,
            ``'EQ_ABORT'``, or ``'EQ_TIMEOUT'``. If the query finds work to be done
            then the dictionary will be:  ``{'type': 'work', 'eq_task_id': eq_task_id,
            'payload': P}`` where ``P`` is the parameters for the work to be done.
        """
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    status, result = self.pop_out_queue(cur, eq_type, n, delay, timeout)
                    self.logger.info(f'MSG: {status} {result}')
                    if status == ResultStatus.SUCCESS:
                        eq_task_ids = result
                        payloads = self.select_task_payload(cur, eq_task_ids, worker_pool)
                        results = []
                        for task_id, payload in payloads:
                            if payload == EQ_STOP:
                                results.append({'type': 'status', 'payload': EQ_STOP})
                            else:
                                results.append({'type': 'work', 'eq_task_id': task_id,
                                                'payload': payload})
                        if n == 1:
                            return results[0]
                        else:
                            return results
                    else:
                        # timed out
                        return {'type': 'status', 'payload': result}
        except Exception:
            return {'type': 'status', 'payload': EQ_ABORT}

    def report_task(self, eq_task_id: int, eq_type: int, result: str) -> ResultStatus:
        """Reports the result of the specified task of the specified type

        Args:
            eq_task_id: the id of the task whose results are being reported.
            eq_type: the type of the task whose results are being reported.
            result: the result of the task.
        Returns:
            :py:class:`ResultStatus.SUCCESS` if the task was successfully reported, otherwise
            :py:class:`ResultStatus.FAILURE`.
        """
        # We do this is in two transactions so if push_in_queue fails, we don't
        # rollback update_task and lose a task result.
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    self.update_task(cur, eq_task_id, result)
        except Exception:
            self.logger.error(f'report_task error {traceback.format_exc()}')
            return ResultStatus.FAILURE

        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    return self.push_in_queue(cur, eq_task_id, eq_type)
        except Exception:
            self.logger.error(f'report_task error {traceback.format_exc()}')
            return ResultStatus.FAILURE

    def are_queues_empty(self, eq_type: int = None) -> bool:
        """Returns whether or not either of the input or output queues are empty,
        optionally of a specified task type.

        Args:
            eq_type: the optional task type to check for.

        Returns:
            True if the queues are empty, otherwise False.
        """
        empty = True
        suffix = ''
        if eq_type is not None:
            suffix = f' where eq_task_type = {eq_type}'
        with self.db.conn:
            with self.db.conn.cursor() as cur:
                tables = ["emews_queue_in", "emews_queue_out"]
                for table in tables:
                    cur.execute(f"select count(eq_task_id) from {table}{suffix};")
                    rs = cur.fetchone()
                    count = rs[0]
                    if count > 0:
                        # print(f": There are entries in table '{table}'")
                        empty = False

        return empty

    def clear_queues(self):
        """Clears the input and output queues and sets the status of those tasks in the
        tasks table to CANCELED.

        **NOTE**: this is only a convenience method for resetting the queues to a coherent
        starting state, and should **NOT** be used to cancel tasks.
        """
        with self.db.conn:
            with self.db.conn.cursor() as cur:
                tables = ["emews_queue_in", "emews_queue_out"]
                for table in tables:
                    # postgres specific SQL
                    update_query = f'update eq_tasks set eq_status = {TaskStatus.CANCELED.value} from '\
                        f'(select eq_task_id from {table}) as cleared_tasks where '\
                        'eq_tasks.eq_task_id = cleared_tasks.eq_task_id'
                    cur.execute(update_query)
                    cur.execute(f'delete from {table}')

    def _query_status(self, eq_task_ids: Iterable[int]) -> List[Tuple[int, TaskStatus]]:
        """Queries for the status (queued, running, etc.) of the specified tasks

        Args:
            eq_task_ids: the ids of the tasks to query the status of

        Returns:
            A List of Tuples containing the status of the tasks. The first element
            of the tuple will be the task id, and the second element will be the
            status of that task as a :py:class:`TaskStatus` object.
        """
        ids = tuple(eq_task_ids)
        placeholders = ', '.join(['%s'] * len(ids))
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    query = f'select eq_task_id, eq_status from eq_tasks where eq_task_id in ({placeholders})'
                    cur.execute(query, ids)
                    results = [(eq_task_id, TaskStatus(status)) for eq_task_id, status in cur.fetchall()]
        except Exception:
            self.logger.error(f'query_status error: {traceback.format_exc()}')
            return None

        return results

    def get_status(self, futures: Iterable[Future]) -> List[Tuple[Future, TaskStatus]]:
        """Gets the status (queued, running, etc.) of the specified tasks

        Args:
            futures: the futures of the tasks to get the status of.

        Returns:
            A List of Tuples containing the status of the tasks. The first element
            of the tuple will be the task id, and the second element will be the
            status of that task as a :py:class:`TaskStatus` object.
        """
        id_map = {ft.eq_task_id: ft for ft in futures}
        result = self._query_status(id_map.keys())
        if result is not None:
            return [(id_map[item[0]], item[1]) for item in result]
        return result

    def _cancel_tasks(self, eq_task_ids: Iterable[int]) -> Tuple[ResultStatus, int]:
        """Cancels the specified tasks by removing them from the output queue and
        marking their status as canceled.

        Args:
            eq_task_ids: the ids of the tasks to cancel.

        Returns:
            If the cancel is successful, the Tuple will contain ResultStatus.SUCCESS and
            the ids of the canceled tasks, otherwise (ResultStatus.FAILURE, []).
        """
        ids = tuple(eq_task_ids)
        placeholders = ', '.join(['%s'] * len(ids))
        # delete should lock all the rows, so they can't be selected
        delete_query = f'delete from emews_queue_out where eq_task_id in ({placeholders}) returning eq_task_id;'
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    cur.execute(delete_query, ids)
                    deleted_ids = [row[0] for row in cur.fetchall()]
                    deleted_placeholders = ', '.join(['%s'] * len(deleted_ids))
                    update_query = f'update eq_tasks set eq_status = {TaskStatus.CANCELED.value} where '\
                                   f'eq_task_id in ({deleted_placeholders});'
                    cur.execute(update_query, deleted_ids)

        except Exception:
            self.logger.error(f'cancel task error: {traceback.format_exc()}')
            return (ResultStatus.FAILURE, [])

        return (ResultStatus.SUCCESS, deleted_ids)

    def _update_status(self, eq_task_ids: Iterable[int], status: TaskStatus):
        ids = tuple(eq_task_ids)
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    placeholders = ', '.join(['%s'] * len(ids))
                    query = f'update eq_tasks set eq_status = {int(status)} where eq_task_id in ({placeholders})'
                    cur.execute(query, ids)
        except Exception:
            self.logger.error(f'update status error: {traceback.format_exc()}')
            return ResultStatus.FAILURE

        return ResultStatus.SUCCESS

    def _update_priorities(self, eq_task_ids: Iterable[int], new_priority: Union[int, List[int]]) -> Tuple[ResultStatus, int]:
        """Updates the priorities of the specified tasks.

        Args:
            eq_task_ids: the ids of the tasks whose priorities should be updated.
            new_priority: the new priority for the specified tasks. If this is a single integer then
            all the specified tasks are updated with that priority. If this is a
            List of ints then each task is updated with the corresponding priority, i.e.,
            the first task in the eq_task_ids is updated with the first priority in the new_priority
            List.

        Returns:
            If the update is successful, the Tuple will contain ResultStatus.SUCCESS and
            the eq_task_ids of the tasks whose priority was successfully updated,
            otherwise (ResultStatus.FAILURE, []).
        """

        # update emews_queue_out as u set
        # eq_priority = u2.priority
        # from (values
        # (1, 3),
        # (2, 5),
        # (3, 2)) as u2(id, priority) where u2.id = eq_task_id;
        ids = tuple(eq_task_ids)
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    if isinstance(new_priority, int):
                        placeholders = ', '.join(['%s'] * len(ids))
                        query = f'update emews_queue_out set eq_priority = %s where eq_task_id in ({placeholders}) returning eq_task_id'
                        cur.execute(query, (new_priority,) + ids)
                        affected_ids = tuple([x[0] for x in cur.fetchall()])
                        if len(affected_ids) > 0:
                            placeholders = ', '.join(['%s'] * len(affected_ids))
                            query = f'update eq_tasks set eq_priority = %s where eq_task_id in ({placeholders})'
                            cur.execute(query, (new_priority,) + affected_ids)
                    else:
                        if len(ids) != len(new_priority):
                            raise ValueError("Number of task ids and updated priorities must be equal")
                        placeholders = ', '.join(['(%s, %s)'] * len(ids))
                        query = f"""update emews_queue_out as u set
                                eq_priority = u2.priority
                                from (values
                                {placeholders}
                                ) as u2(id, priority) where u2.id = eq_task_id returning eq_task_id;
                                """
                        cur.execute(query, [y for x in zip(ids, new_priority) for y in x])
                        affected_ids = tuple([x[0] for x in cur.fetchall()])
                        aff_set = set(affected_ids)
                        update_values = []
                        if len(affected_ids) > 0:
                            for i, eq_id in enumerate(ids):
                                if eq_id in aff_set:
                                    update_values.append(eq_id)
                                    update_values.append(new_priority[i])

                            placeholders = ', '.join(['(%s, %s)'] * len(affected_ids))
                            query = f"""update eq_tasks as u set
                                eq_priority = u2.priority
                                from (values
                                {placeholders}
                                ) as u2(id, priority) where u2.id = eq_task_id returning eq_task_id;
                                """
                            cur.execute(query, update_values)

        except Exception:
            self.logger.error(f'update_priority error: {traceback.format_exc()}')
            return (ResultStatus.FAILURE, [])

        return (ResultStatus.SUCCESS, affected_ids)

    def update_priorities(self, futures: List[Future], new_priority: Union[int, List[int]]) -> Tuple[ResultStatus, int]:
        """Updates the priority of the specified :py:class:`Futures <Future>` to the new_priority.

        Args:
            futures: the :py:class:`Futures <Future>` to update.
            new_priority: the priority to update to. If this is a single integer then
                all the specified tasks are updated to that priority. If this is a
                List of ints then each task is updated with the corresponding priority, i.e.,
                the first task in the ``futures`` is updated with the first priority in the new_priority
                List.

        Returns:
            If the update is successful, the Tuple will contain ResultStatus.SUCCESS and
                the eq_task_ids of the tasks whose priority was successfully updated,
                otherwise (ResultStatus.FAILURE, []).
        """
        return self._update_priorities((ft.eq_task_id for ft in futures), new_priority)

    def _get_priorities(self, eq_task_ids: Iterable[int]) -> List[Tuple[int, int]]:
        ids = tuple(eq_task_ids)
        placeholders = ', '.join(['%s'] * len(ids))
        results = []

        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    query = f'select eq_task_id, eq_priority from eq_tasks where eq_task_id in ({placeholders})'
                    cur.execute(query, ids)
                    for eq_task_id, priority in cur.fetchall():
                        results.append((eq_task_id, priority))
        except Exception:
            self.logger.error(f'query_priority error: {traceback.format_exc()}')
            return None

        return results

    def get_priorities(self, futures: Iterable[Future]) -> List[Tuple[Future, int]]:
        """Gets the priorities of the specified tasks.

        Args:
            futures: the futures of the tasks whose priorities are returned.

        Returns:
            A List of tuples containing the future and priorty for each task, or ResultStatus.FAILURE
            if the query has failed.
        """
        id_map = {ft.eq_task_id: ft for ft in futures}
        results = self._get_priorities(id_map.keys())
        if results is None:
            return ResultStatus.FAILURE

        return [(id_map[eq_task_id], priority) for eq_task_id, priority in results]

    def query_result(self, eq_task_id: int, delay: float = 0.5, timeout: float = 2.0) -> Tuple[ResultStatus, str]:
        """Queries for the result of the specified task.

        The query repeatedly polls for a result. The polling interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is increased after the first poll. The polling will
        timeout after the amount of time specified by the timout value is has elapsed.

        Args:
            eq_task_id: the id of the task to query
            delay: the initial polling delay value
            timeout: the duration after which the query will timeout. If timeout is None, there is no limit to
                the wait time.

        Returns:
            A tuple whose first element indicates the status of the query:
            ``ResultStatus.SUCCESS`` or ``ResultStatus.FAILURE``, and whose second element
            is either the result of the task, or in the case of failure the reason
            for the failure (``EQ_TIMEOUT``, or ``EQ_ABORT``)
        """
        try:
            with self.db.conn:
                with self.db.conn.cursor() as cur:
                    msg = self.pop_in_queue(cur, eq_task_id, delay, timeout)
                    if msg[0] != ResultStatus.SUCCESS:
                        return msg

                    return (ResultStatus.SUCCESS, self.select_task_result(cur, eq_task_id))
        except Exception:
            self.logger.error(f'query_result error {traceback.format_exc()}')
            return (ResultStatus.FAILURE, EQ_ABORT)

    def cancel_tasks(self, futures: List[Future]) -> Tuple[ResultStatus, int]:
        """Cancels the specified :py:class:`Futures <Future>`.

        Args:
            futures: the :py:class:`Futures <Future>` to cancel.

        Returns:
            A tuple containing the :py:class:`ResultStatus` and the ids of the successfully canceled tasks.
        """
        result = self._cancel_tasks(ft.eq_task_id for ft in futures)
        ft_map = {ft.eq_task_id: ft for ft in futures}
        if result[0] == ResultStatus.SUCCESS:
            for eq_task_id in result[1]:
                ft_map[eq_task_id]._task_status = TaskStatus.CANCELED

        return result

    def _get_worker_pools(self, eq_task_ids: Tuple[int],
                          id_map: Dict[int, Future] = None) -> List[Tuple[Union[int, Future], Union[str, None]]]:

        placeholders = ', '.join(['%s'] * len(eq_task_ids))
        with self.db.conn:
            with self.db.conn.cursor() as cur:
                query = f'select eq_task_id, worker_pool from eq_tasks where eq_task_id in ({placeholders})'
                cur.execute(query, eq_task_ids)
                if id_map is None:
                    return [(eq_task_id, wp) for eq_task_id, wp in cur.fetchall()]
                else:
                    return [(id_map[eq_task_id], wp) for eq_task_id, wp in cur.fetchall()]

    def get_worker_pools(self, futures: List[Future]) -> List[Tuple[Future, Union[str, None]]]:
        """Gets the worker pools on which the specified list of :py:class:`Futures <Future>` are running, if any.
        All the specified :py:class:`Futures <Future>` must have been submitted by the same task queue (i.e., they
        are all in the same eqsql database).

        Returns:
            A list of two element tuples. The first element is a :py:class:`Future`, and the
            second is that :py:class:`Futures <Future>`'s worker pool, or None, if the :py:class:`Future` hasn't been
            selected for execution yet.
        """
        id_map = {ft.eq_task_id: ft for ft in futures}
        ids = tuple(ft.eq_task_id for ft in futures)
        try:
            return self._get_worker_pools(ids, id_map)
        except Exception:
            self.logger.error(f'query_worker_pool error: {traceback.format_exc()}')
            return None

    def pop_completed(self, futures: List[Future], timeout=None, sleep: float = 0) -> Future:
        """Pops and returns the first completed future from the specified List
        of Futures.

        Args:
            futures: the List of :py:class:`Futures <Future>` to check for a completed one. The completed
                :py:class:`Future` will be popped from this list.
            timeout: a :py:class:`TimeoutError` will be raised if a completed Future cannot be returned
                by after this amount time.
            sleep: the time, in seconds, to sleep between each iteration over all the Futures,
                when looking for a completed one.

        Returns:
            The first completed :py:class:`Future` from the specified List
            of :py:class:`Futures <Future>`.
        """
        f = next(self.as_completed(futures, pop=True, timeout=timeout, n=1, sleep=sleep))
        return f

    def as_completed(self, futures: List[Future], pop: bool = False, timeout: float = None, n: int = None,
                     batch_size: int = 1, sleep: float = 0) -> Generator[Future, None, None]:
        """Returns a generator over the :py:class:`Futures <Future>` in the ``futures`` argument that yields
        Futures as they complete. The  :py:class:`Futures <Future>` are checked for completion by iterating over all of the
        ones that have not yet completed and checking for a result. At the end of each iteration, the
        ``timeout`` is checked. Note that adding or removing :py:class:`Futures <Future>`
        to or from the ``futures`` argument List while iterating may have no effect on this call.
        A :py:class:`TimeoutError` will be raised if the futures do not complete within the specified ``timeout`` duration.

        Args:
            futures: the List of  :py:class:`Futures <Future>` to iterate over and return as they complete.
            pop: if true, completed futures will be popped off of the futures argument List.
            timeout: if the time taken for futures to completed is greater than this value, then
                raise :py:class:`TimeoutError`.
            n: yield this many completed Futures and then stop iteration.
            batch_size: retrieve this many completed futures, before yielding.
            sleep: the time, in seconds, to sleep between each iteration over all the Futures.

        Yields:
        :py:class:`Futures <Future>` in the ``futures`` argument as they complete.

        Examples:
            >>> futures = []
                // submit some tasks and append the Future instances to futures
                for ft in task_queue.as_completed(futures, timeout=5):
                    status, result = ft.result()
                    // do something with result
        """
        start_time = time.time()
        completed_tasks = set()
        wk_futures = [f for f in futures]
        n_futures = len(wk_futures)
        n_completed = 0
        batch = []
        n_batch = 0

        while True:
            for f in wk_futures:
                if f.eq_task_id not in completed_tasks:
                    status, result_str = f.result(timeout=0.0)
                    if status == ResultStatus.SUCCESS or result_str == EQ_ABORT:
                        completed_tasks.add(f.eq_task_id)
                        batch.append(f)
                        n_batch += 1

                        if n_batch == batch_size:
                            for ft in batch:
                                if pop:
                                    futures.remove(ft)
                                yield ft

                            batch.clear()
                            n_batch = 0

                        n_completed = len(completed_tasks)
                        if n_completed == n_futures or n_completed == n:
                            if n_batch > 0:
                                for ft in batch:
                                    if pop:
                                        futures.remove(ft)
                                    yield ft

                            # Python docs: return rather than raise StopIteration
                            return

                if timeout is not None and time.time() - start_time > timeout:
                    raise TimeoutError(f'as_completed timed out after {timeout} seconds')

            if sleep > 0:
                time.sleep(sleep)


def init_task_queue(host: str, user: str, port: int, db_name: str, password: str = None, retry_threshold=0,
                    log_level=logging.WARN) -> TaskQueue:
    """Initializes and returns an :py:class:`LocalTaskQueue` class instance with the specified parameters.

    Args:
        host: the eqsql database host
        user: the eqsql database user
        port: the eqsql database port
        db_name: the eqsql database name
        password: the eqdql database password (if there is one)
        retry_threshold: if a DB connection cannot be established
            (e.g, there are currently too many connections),
            then retry ``retry_threshold`` many times to establish a connection. There
            will be random few second delay betwen each retry.
        log_level: the logging threshold level.
    Returns:
        An :py:class:`LocalTaskQueue` instance
    """

    global _log_id
    log_name = f'{__name__}-{_log_id}'
    _log_id += 1
    logger = db_tools.setup_log(log_name, log_level)

    retries = 0
    while True:
        try:
            db = db_tools.WorkflowSQL(host=host, user=user, port=port, dbname=db_name, password=password,
                                      log_level=log_level, envs=False)
            db.connect()
            break
        except db_tools.ConnectionException as e:
            retries += 1
            if retries > retry_threshold:
                raise e
            time.sleep(random() * 4)

    return LocalTaskQueue(db, logger)
