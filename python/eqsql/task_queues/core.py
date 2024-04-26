"""Task Queue protocol and implementations"""
from typing import Protocol, Tuple, Union, List, Generator, Iterable
from typing import runtime_checkable

from enum import IntEnum
import json


class ResultStatus(IntEnum):
    """Enum defining the status (success or failure) of an EQSQL database
    operation.
    """
    SUCCESS = 0
    FAILURE = 1


EQ_ABORT = 'EQ_ABORT'
EQ_TIMEOUT = 'EQ_TIMEOUT'
EQ_STOP = 'EQ_STOP'

ABORT_MSG = json.dumps({'type': 'status', 'payload': EQ_ABORT})


class TaskStatus(IntEnum):
    """Enum defining the status of a task: queued, etc. These are used
    in the database to store the status of a task.
    """
    QUEUED = 0
    RUNNING = 1
    COMPLETE = 2
    CANCELED = 3
    REQUEUED = 4


class Future:

    def __init__(self, eq_sql: 'TaskQueue', eq_task_id: int, tag: str = None):
        """Represents the eventual result of an LocalTaskQueue task. Future
        instances are returned by the :py:class:`LocalTaskQueue.submit_task`, and
        :py:class:`LocalTaskQueue.submit_tasks` methods.

        Args:
            eq_sql: the LocalTaskQueue instance that created this Future.
            eq_task_id: the task id
            tag: an optional metadata tag
        """
        self.eq_task_id = eq_task_id
        self.tag = tag
        self.eq_sql = eq_sql
        self._result = None
        self._task_status: Union[TaskStatus, None] = None
        self._pool = None

    def result(self, delay: float = 0.5, timeout: float = 2.0) -> Tuple[ResultStatus, str]:
        """Gets the result of this future task.

        This repeatedly pools the DB for the task result. The polling interval is specified by
        the delay such that the first interval is defined by the initial delay value
        which is increased after the first poll. The polling will
        timeout after the amount of time specified by the timout value is has elapsed.

    Args:
        delay: the initial polling delay value
        timeout: the duration after which the query will timeout.
            If timeout is None, there is no limit to the wait time.

    Returns:
        A tuple whose first element indicates the status of the query:
        :py:class:`ResultStatus.SUCCESS` or :py:class:`ResultStatus.FAILURE`, and whose second element
        is either the result of the task, or in the case of failure the reason
        for the failure (``EQ_TIMEOUT``, or ``EQ_ABORT``)
        """
        # retry after an abort
        if self._result is None or self._result[1] == EQ_ABORT:
            status_result = self.eq_sql.query_result(self.eq_task_id, delay, timeout=timeout)
            if status_result[0] == ResultStatus.SUCCESS or status_result[1] == EQ_ABORT:
                self._result = status_result

            return status_result

        return self._result

    @property
    def status(self) -> TaskStatus:
        """Gets the current status of this Future, one of :py:class:`TaskStatus.QUEUED`,
        :py:class:`TaskStatus.RUNNING`, :py:class:`TaskStatus.COMPLETE`, or :py:class:`TaskStatus.CANCELED`.

        Returns:
            One of :py:class:`TaskStatus.QUEUED`, :py:class:`TaskStatus.RUNNING`, :py:class:`TaskStatus.COMPLETE`,
            :py:class:`TaskStatus.CANCELED`, or ``None`` if the status query fails.
        """
        if self._task_status is None:
            result = self.eq_sql.get_status([self])
            if result is None:
                return result
            else:
                ts = result[0][1]
                if ts == TaskStatus.COMPLETE or ts == TaskStatus.CANCELED:
                    self._task_status = ts
                return ts

        return self._task_status

    @property
    def worker_pool(self) -> Union[str, None]:
        """Gets the id of the worker pool, if any, that this Future task is
        running on.

        Returns:
            The id of the worker pool that this Future task is
            running on, or ``None`` if the task hasn't been selected
            by a worker pool yet.
        """
        if self._pool is None:
            _, pool = self.eq_sql.get_worker_pools([self])[0]
            self._pool = pool
        return self._pool

    def cancel(self):
        """Cancels this Future's task by removing this Future's task id from the output queue.
        Cancelation can fail if this Future task has been popped from the output queue
        before this call completes. Calling this on an already canceled task will return True.

        Returns:
            True if the task is canceled, otherwise False.
        """
        if self.status == TaskStatus.CANCELED:
            return True

        status, ids = self.eq_sql.cancel_tasks([self])
        return status == ResultStatus.SUCCESS and self.eq_task_id in ids

    def done(self):
        """Returns True if this Future task has been completed or canceled, otherwise
        False
        """
        status = self.status
        return status == TaskStatus.CANCELED or status == TaskStatus.COMPLETE

    @property
    def priority(self) -> int:
        """Gets the priority of this Future task.

        Returns:
            The priority of this Future task.
        """
        result = self.eq_sql.get_priorities([self])
        return result if result == ResultStatus.FAILURE else result[0][1]

    @priority.setter
    def priority(self, new_priority) -> ResultStatus:
        """Updates the priority of this Future task.

        Args:
            new_priority: the updated priority
        Returns:
            ResultStatus.SUCCESS if the priority has been successfully updated, otherwise false.
        """
        status, _ = self.eq_sql.update_priorities([self], new_priority)
        return status


# @runtime_checkable
# class Future(Protocol):
#     """Represents the eventual result of an EQSQL task."""

#     def result(self, delay: float = 0.5, timeout: float = 2.0) -> Tuple[ResultStatus, str]:
#         """Gets the result of this future task.

#         This repeatedly pools a DB for the task result. The polling interval is specified by
#         the delay such that the first interval is defined by the initial delay value
#         which is increased after the first poll. The polling will
#         timeout after the amount of time specified by the timout value is has elapsed.

#         Args:
#             delay: the initial polling delay value
#             timeout: the duration after which the query will timeout.
#                 If timeout is None, there is no limit to the wait time.

#         Returns:
#             A tuple whose first element indicates the status of the query:
#             :py:class:`ResultStatus.SUCCESS` or :py:class:`ResultStatus.FAILURE`, and whose second element
#             is either the result of the task, or in the case of failure the reason
#             for the failure (``EQ_TIMEOUT``, or ``EQ_ABORT``)
#             """

#     @property
#     def status(self) -> TaskStatus:
#         """Gets the current status of this Future, one of :py:class:`TaskStatus.QUEUED`,
#         :py:class:`TaskStatus.RUNNING`, :py:class:`TaskStatus.COMPLETE`, or :py:class:`TaskStatus.CANCELED`.

#         Returns:
#             One of :py:class:`TaskStatus.QUEUED`, :py:class:`TaskStatus.RUNNING`, :py:class:`TaskStatus.COMPLETE`,
#             :py:class:`TaskStatus.CANCELED`, or ``None`` if the status query fails.
#         """

#     @property
#     def worker_pool(self) -> Union[str, None]:
#         """Gets the id of the worker pool, if any, that this Future task is
#         running on.

#         Returns:
#             The id of the worker pool that this Future task is
#             running on, or ``None`` if the task hasn't been selected
#             by a worker pool yet.
#         """

#     def cancel(self) -> bool:
#         """Cancels this Future's task by removing this Future's task id from the output queue.
#         Cancelation can fail if this Future task has been popped from the output queue
#         before this call completes. Calling this on an already canceled task will return True.

#         Returns:
#             True if the task is canceled, otherwise False.
#         """

#     def done(self) -> bool:
#         """Returns True if this Future task has been completed or canceled, otherwise
#         False
#         """

#     @property
#     def priority(self) -> int:
#         """Gets the priority of this Future task.

#         Returns:
#             The priority of this Future task.
#         """

#     @priority.setter
#     def priority(self, new_priority) -> ResultStatus:
#         """Updates the priority of this Future task.

#         Args:
#             new_priority: the updated priority
#         Returns:
#             ResultStatus.SUCCESS if the priority has been successfully updated, otherwise false.
#         """


@runtime_checkable
class TaskQueue(Protocol):
    """Task queue protocol for submitting, manipulating and
    retrieving tasks"""

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

    def submit_tasks(self, exp_id: str, eq_type: int, payload: List[str], priority: int = 0,
                     tag: str = None) -> Tuple[ResultStatus, List[Future]]:
        """Submits work of the specified type and priority with the specified
        payloads, returning the :py:class:`status <ResultStatus>` and the :py:class:`futures <Future>`
        encapsulating the submission.

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

    def cancel_tasks(self, futures: List[Future]) -> Tuple[ResultStatus, int]:
        """Cancels the specified :py:class:`Futures <Future>`.

        Args:
            futures: the :py:class:`Futures <Future>` to cancel.

        Returns:
            A tuple containing the :py:class:`ResultStatus` and the ids of the successfully canceled tasks.
        """

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

    def get_priorities(self, futures: Iterable[Future]) -> List[Tuple[Future, int]]:
        """Gets the priorities of the specified tasks.

        Args:
            futures: the futures of the tasks whose priorities are returned.

        Returns:
            A List of tuples containing the future and priorty for each task, or ResultStatus.FAILURE
            if the query has failed.
        """

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

    def are_queues_empty(self, eq_type: int = None) -> bool:
        """Returns whether or not either of the input or output queues are empty,
        optionally of a specified task type.

        Args:
            eq_type: the optional task type to check for.

        Returns:
            True if the queues are empty, otherwise False.
        """

    def get_worker_pools(self, futures: List[Future]) -> List[Tuple[Future, Union[str, None]]]:
        """Gets the worker pools on which the specified list of :py:class:`Futures <Future>` are running, if any.
        All the specified :py:class:`Futures <Future>` must have been submitted by the same task queue (i.e., they
        are all in the same eqsql database).

        Returns:
            A list of two element tuples. The first element is a :py:class:`Future`, and the
            second is that :py:class:`Futures <Future>`'s worker pool, or None, if the :py:class:`Future` hasn't been
            selected for execution yet.
        """

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

    def get_status(self, futures: Iterable[Future]) -> List[Tuple[Future, TaskStatus]]:
        """Gets the status (queued, running, etc.) of the specified tasks

        Args:
            futures: the futures of the tasks to get the status of.

        Returns:
            A List of Tuples containing the status of the tasks. The first element
            of the tuple will be the task id, and the second element will be the
            status of that task as a :py:class:`TaskStatus` object.
        """


class StopConditionException(Exception):
    def __init__(self, msg='StopIterationException', *args, **kwargs):
        super().__init__(msg, *args, **kwargs)
        pass


class TimeoutError(Exception):
    def __init__(self, msg='TimeoutError', *args, **kwargs):
        """Exception used to indicate that a query has timed out."""
        super().__init__(msg, *args, **kwargs)
        pass
