
# EQ-SQL eq.py

import random
import sys
import threading
# import importlib
import traceback
import time

import db_tools

EQPY_ABORT = "EQPY_ABORT"

try:
    import queue as q
except ImportError:
    # queue is Queue in python 2
    import Queue as q

input_q = q.Queue()
output_q = q.Queue()

p = None
aborted = False
wait_info = None

# The psycopg2 handle:
DB = None


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


def init():
    global DB
    DB = db_tools.setup_db(envs=True)
    DB.connect()


def output_q_get():
    global output_q, aborted
    wait = wait_info.getWait()
    # thread's runnable might put work on queue
    # and finish, so it would not longer be alive
    # but something remains on the queue to be pulled
    while p.is_alive() or not output_q.empty():
        try:
            result = output_q.get(True, wait)
            break
        except q.Empty:
            pass
    else:
        # if we haven't yet set the abort flag then
        # return that, otherwise return the formated exception
        if aborted:
            result = p.exc
        else:
            result = EQPY_ABORT
        print("EXCEPTION: " + str(p.exc))
        aborted = True

    return result


def sql_pop_q(table):
    """
    Generate code for a queue pop from given table
    From: https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    """
    code = """
    DELETE FROM emews_queue_OUT
    WHERE eq_id = (
    SELECT eq_id
    FROM %s
    ORDER BY eq_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    """ % table
    return code


def OUT_put(string_params):
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    id = rs[0]
    # V = db_tools.sql_tuple([str(id), "'M'"])
    # print("V: " + str(V))
    DB.insert("emews_queue_OUT", ["eq_id", "json"],
              [str(id), db_tools.q(string_params)])


def OUT_get(delay=0.1, timeout=1.0):
    global DB
    sql_pop = sql_pop_q("emews_queue_OUT")
    start = time.time()
    while True:
        DB.execute(sql_pop)
        rs = DB.get()
        if rs is not None:
            break  # got good data
        if time.time() - start > timeout:
            break  # timeout
        delay = delay * random.random() * 2
        time.sleep(delay)
        # print("OUT_get(): " + str(delay))
        sys.stdout.flush()
        delay = delay * 2

    print("OUT_get(): " + str(rs))
    sys.stdout.flush()
    if rs is None: return None
    params = rs[1]
    return params


def IN_get():
    result = input_q.get()
    return result
