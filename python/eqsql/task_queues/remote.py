"""Remote task queue implementations."""
from typing import Tuple, Union, List, Generator, Iterable
from globus_compute_sdk import Executor
from dataclasses import dataclass

from eqsql.task_queues.core import ResultStatus, TaskStatus, Future


@dataclass
class DBParameters:
    user: str
    host: str
    db_name: str
    port: int = None
    retry_threshold: int = 10


def _submit_tasks(db_params: DBParameters, exp_id: str, eq_type: int, payload: List[str], priority: int = 0,
                  tag: str = None):
    from eqsql.task_queues import local
    task_queue = local.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                       retry_threshold=db_params.retry_threshold)
    result_status, fts = task_queue.submit_tasks(exp_id, eq_type, payload, priority, tag)
    task_queue.close()
    return (result_status, [ft.eq_task_id for ft in fts])


class GCTaskQueue:
    """Task queue protocol for submitting, manipulating and
    retrieving tasks"""

    def __init__(self, gcx: Executor, db_params):
        self.db_params = db_params
        self.gcx = gcx

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
        gc_ft = self.gcx.submit(_submit_tasks, self.db_params, exp_id, eq_type, [payload], priority,
                                tag)
        status, task_ids = gc_ft.result()
        if status == ResultStatus.SUCCESS:
            return (status, Future(self, task_ids[0], tag))
        else:
            return (status, None)

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
        gc_ft = self.gcx.submit(_submit_tasks, self.db_params, exp_id, eq_type, payload, priority,
                                tag)
        status, task_ids = gc_ft.result()
        if status == ResultStatus.SUCCESS:
            return (status, [Future(self, task_id, tag) for task_id in task_ids])

    def cancel_tasks(self, futures: List[Future]) -> Tuple[ResultStatus, int]:
        """Cancels the specified :py:class:`Futures <Future>`.

        Args:
            futures: the :py:class:`Futures <Future>` to cancel.

        Returns:
            A tuple containing the :py:class:`ResultStatus` and number of tasks successfully canceled.
        """
        pass

    def get_priorities(self, eq_task_ids: Iterable[Future]) -> List[Tuple[Future, int]]:
        """Gets the priorities of the specified tasks.

        Args:
            futures: the futures of the tasks whose priorities are returned.

        Returns:
            A List of tuples containing the future and priorty for each task, or None if the
            query has failed.
        """
        pass

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
        pass

    def are_queues_empty(self, eq_type: int = None) -> bool:
        """Returns whether or not either of the input or output queues are empty,
        optionally of a specified task type.

        Args:
            eq_type: the optional task type to check for.

        Returns:
            True if the queues are empty, otherwise False.
        """
        pass

    def get_worker_pools(self, futures: List[Future]) -> List[Tuple[Future, Union[str, None]]]:
        """Gets the worker pools on which the specified list of :py:class:`Futures <Future>` are running, if any.
        All the specified :py:class:`Futures <Future>` must have been submitted by the same task queue (i.e., they
        are all in the same eqsql database).

        Returns:
            A list of two element tuples. The first element is a :py:class:`Future`, and the
            second is that :py:class:`Futures <Future>`'s worker pool, or None, if the :py:class:`Future` hasn't been
            selected for execution yet.
        """
        pass

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
        pass

    def as_completed(self, futures: List[Future], pop: bool = False, timeout: float = None, n: int = None,
                     sleep: float = 0) -> Generator[Future, None, None]:
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
        pass

    def get_status(self, futures: Iterable[Future]) -> List[Tuple[Future, TaskStatus]]:
        """Gets the status (queued, running, etc.) of the specified tasks

        Args:
            futures: the futures of the tasks to get the status of.

        Returns:
            A List of Tuples containing the status of the tasks. The first element
            of the tuple will be the task id, and the second element will be the
            status of that task as a :py:class:`TaskStatus` object.
        """
        pass


def init_task_queue(gcx: Executor, host: str, user: str, port: int, db_name: str, retry_threshold=0) -> GCTaskQueue:
    """Initializes and returns an :py:class:`LocalTaskQueue` class instance with the specified parameters.

    Args:
        host: the eqsql database host
        user: the eqsql database user
        port: the eqsql database port
        db_name: the eqsql database name
        retry_threshold: if a DB connection cannot be established
            (e.g, there are currently too many connections),
            then retry ``retry_threshold`` many times to establish a connection. There
            will be random few second delay betwen each retry.
    Returns:
        An :py:class:`GCTaskQueue` instance
    """
    db_params = DBParameters(user, host, db_name, port, retry_threshold)
    return GCTaskQueue(gcx, db_params)
