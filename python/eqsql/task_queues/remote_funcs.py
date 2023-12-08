
from typing import Tuple, Union, List
from dataclasses import dataclass
import time

from eqsql.task_queues.core import ResultStatus, EQ_ABORT, TimeoutError


@dataclass
class DBParameters:
    user: str
    host: str
    db_name: str
    port: int = None
    retry_threshold: int = 10

    def to_dict(self):
        return {'user': self.user, 'host': self.host, 'db_name': self.db_name,
                'port': self.port, 'retry': self.retry_threshold}

    @staticmethod
    def from_dict(vals):
        return DBParameters(vals['user'], vals['host'], vals['db_name'],
                            vals['port'], vals['retry'])


def _submit_tasks(db_params: DBParameters, exp_id: str, eq_type: int, payload: List[str], priority: int = 0,
                  tag: str = None):
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    result_status, fts = task_queue.submit_tasks(exp_id, eq_type, payload, priority, tag)
    task_queue.close()
    return (result_status, [ft.eq_task_id for ft in fts])


def _get_status(db_params: DBParameters, eq_task_ids: List[int]):
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    result = task_queue._query_status(eq_task_ids)
    return result


def _get_priorities(db_params: DBParameters, eq_task_ids: List[int]):
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    result = task_queue._get_priorities(eq_task_ids)
    return result


def _update_priorities(db_params: DBParameters, eq_task_ids: List[int], new_priority: Union[int, List[int]]) -> Tuple[ResultStatus, int]:
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    return task_queue._update_priorities(eq_task_ids, new_priority)


def _query_result(db_params: DBParameters, eq_task_id: int, delay: float = 0.5,
                  timeout: float = 2.0) -> Tuple[ResultStatus, str]:
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    return task_queue.query_result(eq_task_id, delay, timeout)


def _get_worker_pools(db_params: DBParameters, eq_task_ids: Tuple[int]) -> List[Tuple[int, Union[str, None]]]:
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    return task_queue._get_worker_pools(eq_task_ids)


def _cancel_tasks(db_params: DBParameters, eq_task_ids: List[int]):
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    return task_queue._cancel_tasks(eq_task_ids)


def _are_queues_empty(db_params: DBParameters, eq_type: int = None):
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    return task_queue.are_queues_empty(eq_type)


def _as_completed(db_params: DBParameters, eq_task_ids: List[int], timeout: float = None, n: int = None,
                  sleep: float = 0) -> List[Tuple[int, str]]:
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             retry_threshold=db_params.retry_threshold)
    results = []
    n_tasks = len(eq_task_ids)
    completed_tasks = set()
    start_time = time.time()
    while True:
        for eq_task_id in eq_task_ids:
            if eq_task_id not in completed_tasks:
                result_status, result_str = task_queue.query_result(eq_task_id, timeout=0.0)
                if result_status == ResultStatus.SUCCESS or result_str == EQ_ABORT:
                    completed_tasks.add(eq_task_id)
                    query_result = task_queue._query_status([eq_task_id])
                    task_status = None if query_result is None else query_result[0][1]
                    results.append((eq_task_id, task_status, result_status, result_str))
                n_completed = len(completed_tasks)
                if n_completed == n_tasks or n_completed == n:
                    return results

            if timeout is not None and time.time() - start_time > timeout:
                raise TimeoutError(f'as_completed timed out after {timeout} seconds')

        if sleep > 0:
            time.sleep(sleep)
