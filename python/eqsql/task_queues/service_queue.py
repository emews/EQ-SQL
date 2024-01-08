from typing import Tuple, Union, List, Generator, Iterable
import requests
import json
from random import shuffle


from eqsql.task_queues.remote_funcs import DBParameters
from eqsql.task_queues.core import ResultStatus, TaskStatus, Future, TimeoutError


class ServiceTaskQueue:
    """Task queue protocol for submitting, manipulating and
    retrieving tasks"""

    def __init__(self, service_url: str, db_params: DBParameters):
        self.db_params = db_params.to_dict()
        self.api_host = service_url

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
        msg = {'db_params': self.db_params, 'exp_id': exp_id, 'eq_type': eq_type, 'payload': [payload],
               'priority': priority, 'tag': tag}
        api_url = f'{self.api_host}/submit_tasks'
        response = requests.post(api_url, json=json.dumps(msg))
        status, task_ids = response.json()
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
        msg = {'db_params': self.db_params, 'exp_id': exp_id, 'eq_type': eq_type, 'payload': payload,
               'priority': priority, 'tag': tag}
        api_url = f'{self.api_host}/submit_tasks'
        response = requests.post(api_url, json=json.dumps(msg))
        status, task_ids = response.json()
        if status == ResultStatus.SUCCESS:
            return (status, [Future(self, task_id, tag) for task_id in task_ids])

    def cancel_tasks(self, futures: List[Future]) -> Tuple[ResultStatus, int]:
        """Cancels the specified :py:class:`Futures <Future>`.

        Args:
            futures: the :py:class:`Futures <Future>` to cancel.

        Returns:
            A tuple containing the :py:class:`ResultStatus` and the ids of the successfully canceled tasks.
        """
        ft_map = {ft.eq_task_id: ft for ft in futures}
        msg = {'db_params': self.db_params, 'task_ids': [ft.eq_task_id for ft in futures]}
        api_url = f'{self.api_host}/cancel_tasks'
        response = requests.post(api_url, json=json.dumps(msg))
        result = response.json()
        if result[0] == ResultStatus.SUCCESS:
            for eq_task_id in result[1]:
                ft_map[eq_task_id]._task_status = TaskStatus.CANCELED
        return tuple(result)

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
        msg = {'db_params': self.db_params, 'eq_task_id': eq_task_id, 'delay': delay, 'timeout': timeout}
        api_url = f'{self.api_host}/query_result'
        response = requests.post(api_url, json=json.dumps(msg))
        return response.json()[0]

    def get_priorities(self, futures: Iterable[Future]) -> List[Tuple[Future, int]]:
        """Gets the priorities of the specified tasks.

        Args:
            futures: the futures of the tasks whose priorities are returned.

        Returns:
            A List of tuples containing the future and priorty for each task, or ResultStatus.FAILURE
            if the query has failed.
        """
        id_map = {ft.eq_task_id: ft for ft in futures}
        msg = {'db_params': self.db_params, 'task_ids': [ft.eq_task_id for ft in futures]}
        api_url = f'{self.api_host}/get_priorities'
        response = requests.post(api_url, json=json.dumps(msg))
        result = response.json()
        if result['status'] == 'fail':
            return ResultStatus.FAILURE

        return [(id_map[eq_task_id], priority) for eq_task_id, priority in result['result']]

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
        msg = {'db_params': self.db_params, 'task_ids': [ft.eq_task_id for ft in futures], 'new_priority': new_priority}
        api_url = f'{self.api_host}/update_priorities'
        response = requests.post(api_url, json=json.dumps(msg))
        result = response.json()
        if result['status'] == "fail":
            return (ResultStatus.FAILURE, [])
        return result['result']

    def are_queues_empty(self, eq_type: int = None) -> bool:
        """Returns whether or not either of the input or output queues are empty,
        optionally of a specified task type.

        Args:
            eq_type: the optional task type to check for.

        Returns:
            True if the queues are empty, otherwise False.
        """
        msg = {'db_params': self.db_params, 'eq_type': eq_type}
        api_url = f'{self.api_host}/are_queues_empty'
        response = requests.post(api_url, json=json.dumps(msg))
        return response.json()[0] == 1

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
        msg = {'db_params': self.db_params, 'task_ids': ids}
        api_url = f'{self.api_host}/get_worker_pools'
        response = requests.post(api_url, json=json.dumps(msg))
        return [(id_map[eq_task_id], worker_pool) for eq_task_id, worker_pool in response.json()]

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
        id_map = {ft.eq_task_id: ft for ft in futures}
        api_url = f'{self.api_host}/as_completed'
        n_completed = 0
        n_futures = len(futures)
        completed_tasks = []

        while True:
            eq_task_ids = [ft.eq_task_id for ft in futures]
            # _as_completed iterates through the list of task_ids
            # we shuffle so not biasing the members at the front of the list
            shuffle(eq_task_ids)
            msg = {'db_params': self.db_params, 'task_ids': eq_task_ids, 'timeout': timeout, 'n': n,
                   'sleep': sleep, 'completed_tasks': completed_tasks}
            response = requests.post(api_url, json=json.dumps(msg))
            result = response.json()

            if result['status'] == "timeout_error":
                raise TimeoutError(f'as_completed timed out after {timeout} seconds')

            task_results = result['result']
            task_id, task_status, result_status, task_result = task_results
            completed_tasks.append(task_id)

            ft = id_map[task_id]
            ft._result = (result_status, task_result)
            ft._task_status = TaskStatus.COMPLETE if task_status == TaskStatus.COMPLETE else None
            if pop:
                futures.remove(ft)
            yield ft

            n_completed += 1
            if n_completed == n_futures or n_completed == n:
                # Python docs: return rather than raise StopIteration
                return

    def get_status(self, futures: Iterable[Future]) -> List[Tuple[Future, TaskStatus]]:
        """Gets the status (queued, running, etc.) of the specified tasks

        Args:
            futures: the futures of the tasks to get the status of.

        Returns:
            A List of Tuples containing the status of the tasks. The first element
            of the tuple will be the task id, and the second element will be the
            status of that task as a :py:class:`TaskStatus` object.
        """
        ft_map = {ft.eq_task_id: ft for ft in futures}
        task_ids = [ft.eq_task_id for ft in futures]
        api_url = f'{self.api_host}/get_status'
        msg = {'db_params': self.db_params, 'task_ids': task_ids}
        response = requests.post(api_url, json=json.dumps(msg))
        results = response.json()

        if results is None:
            # TODO: better error handling - logger would have reported error remotely
            return None

        ret = []
        for task_id, status in results:
            t_status = TaskStatus(status)
            ft = ft_map[task_id]
            if t_status == TaskStatus.COMPLETE or t_status == TaskStatus.CANCELED:
                ft._task_status = t_status
            ret.append((ft, t_status))

        return ret


def init_task_queue(service_url: str, db_host: str, db_user: str, db_port: int,
                    db_name: str, password: str = None, retry_threshold=0) -> ServiceTaskQueue:
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
    Returns:
        An :py:class:`ServiceTaskQueue` instance
    """
    db_params = DBParameters(db_user, db_host, db_name, password, db_port, retry_threshold)
    return ServiceTaskQueue(service_url, db_params)
