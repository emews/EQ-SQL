
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
    if DB is not None:
        return
    DB = db_tools.setup_db(envs=True)
    DB.connect()


def validate():
    """ Connect to DB or die! """
    global DB
    # This code has no effect except to validate the connection:
    try:
        DB.execute("select * from emews_id_generator;")
        DB.get()
    except Exception:
        print("ERROR: eq.validate() failed!")
        sys.stdout.flush()
        return None
    return "EQ-SQL:OK"


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


def sql_pop_q(table, eq_type):
    """
    Generate code for a queue pop from given table
    From: https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    """
    code = """
    DELETE FROM %s
    WHERE eq_type = %i AND eq_id = (
    SELECT eq_id
    FROM %s
    ORDER BY eq_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    """ % (table, eq_type, table)
    return code


def queue_pop(table, eq_type, delay, timeout):
    global DB
    sql_pop = sql_pop_q(table, eq_type)
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

    print("queue_pop(%s): '%s'" % (table, str(rs)))
    sys.stdout.flush()
    if rs is None: return None
    result = rs[2]
    return result


def queue_push(table, eq_type, value):
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    id = rs[0]
    # V = db_tools.sql_tuple([str(id), "'M'"])
    # print("V: " + str(V))
    DB.insert(table, ["eq_id", "eq_type", "json"],
                     [str(id),  eq_type, db_tools.q(value)])


def OUT_put(eq_type, params):
    try:
        queue_push("emews_queue_OUT", eq_type, params)
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()


def IN_put(eq_type, params):
    try:
        queue_push("emews_queue_IN", eq_type, params)
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()


def OUT_get(eq_type, delay=0.1, timeout=5.0):
    try:
        result = queue_pop("emews_queue_OUT", eq_type, delay, timeout)
        if result is None:
            print("eq.py:OUT_get(): popped None: abort!")
            sys.stdout.flush()
            result = "EQ_ABORT"
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()
        result = "EQ_ABORT"
    return result


def IN_get(eq_type, delay=0.1, timeout=5.0):
    try:
        result = queue_pop("emews_queue_IN", eq_type, delay, timeout)
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()
        result = "EQ_ABORT"
    return result


def done(msg):
    if msg == "EQ_FINAL":
        return True
    if msg == "EQ_ABORT":
        print("eq.done(): WARNING: EQ_ABORT")
        return True
    return False
