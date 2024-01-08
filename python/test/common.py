import json

from eqsql.task_queues.core import ResultStatus


def create_payload(x=1.2):
    payload = {'x': x, 'y': 7.3, 'z': 'foo'}
    return json.dumps(payload)


# proxy for worker pool reporting back the result of the task
def report_task(db_params, eq_task_id: int, eq_type: int, result: str) -> ResultStatus:
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             password=db_params.password, retry_threshold=db_params.retry_threshold)
    return task_queue.report_task(eq_task_id, eq_type, result)


# proxy for worker pool querying for tasks by type
def query_task(db_params, eq_type, n: int = 1, worker_pool: str = 'default', delay: float = 0.5,
               timeout: float = 2.0):
    from eqsql.task_queues import local_queue
    task_queue = local_queue.init_task_queue(db_params.host, db_params.user, db_params.port, db_params.db_name,
                                             password=db_params.password, retry_threshold=db_params.retry_threshold)
    return task_queue.query_task(eq_type, n, worker_pool, delay, timeout)
