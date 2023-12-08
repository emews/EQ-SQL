from flask import Flask, request
import json
from multiprocessing import Process, Queue

from eqsql.task_queues.remote_funcs import _submit_tasks, _get_status, _get_priorities, _get_worker_pools
from eqsql.task_queues.remote_funcs import _update_priorities, _query_result, _cancel_tasks
from eqsql.task_queues.remote_funcs import _are_queues_empty, _as_completed, DBParameters

app = Flask(__name__)
q: Queue = None


@app.post('/submit_tasks')
def flsk_submit_tasks():
    print("JSON:", request.json)
    # msg = {'exp_id': exp_id, 'eq_type': eq_type, 'payload': [payload], 'priority': priority,
    #        'tag': tag}
    msg = json.loads(request.json)
    db_params = DBParameters.from_dict(msg['db_params'])
    result = _submit_tasks(db_params, msg['exp_id'], msg['eq_type'], msg['payload'],
                           msg['priority'], msg['tag'])
    return result


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
