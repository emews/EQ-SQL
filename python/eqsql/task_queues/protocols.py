"""Task Queue protocol and implementations"""
from typing import Protocol, Tuple, Union, List, Generator
from typing import runtime_checkable

from eqsql.task_queues.common import ResultStatus, TaskStatus


@runtime_checkable
class Future(Protocol):
    """Represents the eventual result of an EQSQL task."""

    def result(self, delay: float = 0.5, timeout: float = 2.0) -> Tuple[ResultStatus, str]:
        """Gets the result of this future task.

        This repeatedly pools a DB for the task result. The polling interval is specified by
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

    @property
    def status(self) -> TaskStatus:
        """Gets the current status of this Future, one of :py:class:`TaskStatus.QUEUED`,
        :py:class:`TaskStatus.RUNNING`, :py:class:`TaskStatus.COMPLETE`, or :py:class:`TaskStatus.CANCELED`.

        Returns:
            One of :py:class:`TaskStatus.QUEUED`, :py:class:`TaskStatus.RUNNING`, :py:class:`TaskStatus.COMPLETE`,
            :py:class:`TaskStatus.CANCELED`, or ``None`` if the status query fails.
        """

    @property
    def worker_pool(self) -> Union[str, None]:
        """Gets the id of the worker pool, if any, that this Future task is
        running on.

        Returns:
            The id of the worker pool that this Future task is
            running on, or ``None`` if the task hasn't been selected
            by a worker pool yet.
        """

    def cancel(self) -> bool:
        """Cancels this Future's task by removing this Future's task id from the output queue.
        Cancelation can fail if this Future task has been popped from the output queue
        before this call completes. Calling this on an already canceled task will return True.

        Returns:
            True if the task is canceled, otherwise False.
        """

    def done(self) -> bool:
        """Returns True if this Future task has been completed or canceled, otherwise
        False
        """

    @property
    def priority(self) -> int:
        """Gets the priority of this Future task.

        Returns:
            The priority of this Future task.
        """

    @priority.setter
    def priority(self, new_priority) -> ResultStatus:
        """Updates the priority of this Future task.

        Args:
            new_priority: the updated priority
        Returns:
            ResultStatus.SUCCESS if the priority has been successfully updated, otherwise false.
        """


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
            A tuple containing the :py:class:`ResultStatus` and number of tasks successfully canceled.
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
            The :py:class:`ResultStatus` and number tasks whose priority was
            successfully updated.
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
                     sleep: float = 0) -> Generator[Future, None, None]:
        """Returns a generator over the :py:class:`Futures <Future>` in the ``futures`` argument that yields
        Futures as they complete. The  :py:class:`Futures <Future>` are checked for completion by iterating over all of the
        ones that have not yet completed and checking for a result. At the end of each iteration, the
        ``stop_condition`` and ``timeout`` are checked. Note that adding or removing :py:class:`Futures <Future>`
        to or from the ``futures`` argument List while iterating may have no effect on this call.
        A :py:class:`TimeoutError` will be raised if the futures do not complete within the specified ``timeout`` duration.
        If the ``stop_condition`` is not ``None``, it will be called after every iteration through the :py:class:`Futures <Future>`.
        If it returns True, then iteration will stop.

        Args:
            futures: the List of  :py:class:`Futures <Future>` to iterate over and return as they complete.
            pop: if true, completed futures will be popped off of the futures argument List.
            timeout: if the time taken for futures to completed is greater than this value, then
                raise :py:class:`TimeoutError`.
            n: yield this many completed Futures and then stop iteration.
            stop_condition: this Callable will be called after each check of all the futures and
                if the return value is True, then iteration will stop.
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
