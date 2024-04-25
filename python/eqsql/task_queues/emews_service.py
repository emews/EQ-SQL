from flask import Flask, request
import json
from multiprocessing import Process, Queue

from eqsql.task_queues.core import TimeoutError, ResultStatus
from eqsql.task_queues.remote_funcs import _submit_tasks, _get_status, _get_priorities, _get_worker_pools
from eqsql.task_queues.remote_funcs import _update_priorities, _query_result, _cancel_tasks
from eqsql.task_queues.remote_funcs import _are_queues_empty, _as_completed, DBParameters

app = Flask(__name__)
q: Queue = None


@app.post('/submit_tasks')
def submit_tasks():
    # msg = {'exp_id': exp_id, 'eq_type': eq_type, 'payload': [payload], 'priority': priority,
    #        'tag': tag}
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _submit_tasks(db_params, msg['exp_id'], msg['eq_type'], msg['payload'],
                           msg['priority'], msg['tag'])
    return list(result)


@app.post('/get_status')
def get_status():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _get_status(db_params, msg['task_ids'])
    return result


@app.post('/get_worker_pools')
def get_worker_pools():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _get_worker_pools(db_params, msg['task_ids'])
    return result


@app.post('/get_priorities')
def get_priorities():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _get_priorities(db_params, msg['task_ids'])
    if result == ResultStatus.FAILURE:
        return {'status': 'fail'}
    return {'status': 'ok', 'result': result}


@app.post('/update_priorities')
def update_priorities():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _update_priorities(db_params, msg['task_ids'], msg['new_priority'])
    if result[0] == ResultStatus.FAILURE:
        return {'status': 'fail'}
    return {'status': 'ok', 'result': result}


@app.post('/cancel_tasks')
def cancel_tasks():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _cancel_tasks(db_params, msg['task_ids'])
    return list(result)


@app.post('/as_completed')
def as_completed():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    try:
        result = _as_completed(db_params, msg['task_ids'], msg['completed_tasks'],
                               msg['timeout'], msg['n_required'], msg['batch_size'], msg['sleep'])
    except TimeoutError:
        return {'status': 'timeout_error'}
    return {'status': 'ok', 'result': result}


@app.post('/query_result')
def query_result():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _query_result(db_params, msg['eq_task_id'], msg['delay'], msg['timeout'])
    return [result]


@app.post('/are_queues_empty')
def are_queues_empty():
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _are_queues_empty(db_params, msg['eq_type'])
    return [1 if result else 0]


@app.get("/shutdown")
def shutdown():
    q.put(1)
    return 'Server shutting down ...'


@app.get("/ping")
def ping():
    return 'pong'


def start(host, port):
    global q
    q = Queue()
    p = Process(target=app.run, args=(host, port))
    # app.run(host=host, port=port)
    p.start()
    q.get()
    p.terminate()
    p.join()
    return host, port


if __name__ == '__main__':
    start('127.0.0.1', 11218)
