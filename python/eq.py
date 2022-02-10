
# EQ-SQL eq.py

import queue
import random
import sys
import threading
# import importlib
import traceback
import time

import db_tools
from db_tools import Q

EQPY_ABORT = "EQPY_ABORT"

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
    DB = db_tools.setup_db(envs=True, log=True)
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
        except queue.Empty:
            pass
    else:
        # if we haven't yet set the abort flag then
        # return that, otherwise return the formatted exception
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
    WHERE  eq_id = (
    SELECT eq_id
    FROM %s
    WHERE eq_type = %i
    ORDER BY eq_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    """ % (table, table, eq_type)
    return code


def queue_pop(table, eq_type, delay, timeout):
    """
    returns (eq_id, eq_type, json_out, json_in) or None on timeout
    """
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
    eq_id = rs[0]
    selection = "eq_id=%i" % eq_id
    DB.select("emews_points", "eq_id,eq_type,json_out,json_in", selection)
    rs = DB.cursor.fetchone()
    if rs is None:
        raise Exception("could not find emews_point: %s\n" %
                        selection)
    result = (rs[0], rs[1], rs[2], rs[3])
    return result


def queue_push(table, eq_id, eq_type):
    DB.insert(table, ["eq_id",    "eq_type"],
                     [str(eq_id),  eq_type])


def DB_submit(eq_type, payload):
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_id = rs[0]
    DB.insert("emews_points", ["eq_id", "eq_type", "json_out"],
                              [ eq_id , eq_type,   Q(payload)])
    OUT_put(eq_id, eq_type)
    return eq_id


def DB_result(eq_id, payload):
    global DB
    DB.update("emews_points", ["json_in"], [Q(payload)],
                              "eq_id=%i" % eq_id)
    IN_put(eq_id, 0)


def DB_final():
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_id = rs[0]
    DB.insert("emews_points", ["eq_id", "eq_type", "json_out"],
                              [ eq_id ,         0, Q("EQ_FINAL")])
    OUT_put(eq_id, 0)
    return eq_id


def OUT_put(eq_id, eq_type):
    try:
        queue_push("emews_queue_OUT", eq_id, eq_type)
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()
    return eq_id


def IN_put(eq_id, eq_type):
    try:
        queue_push("emews_queue_IN", eq_id, eq_type)
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()


def OUT_get(eq_type, delay=0.5, timeout=2.0):
    """ returns tuple: (eq_id, eq_type, json_out) """
    try:
        tpl = queue_pop("emews_queue_OUT", eq_type, delay, timeout)
        if tpl is None:
            print("eq.py:OUT_get(eq_type=%i): popped None: abort!" %
                  eq_type)
            sys.stdout.flush()
            result = (0, 0, "EQ_ABORT")
        else:
            result = (tpl[0], tpl[1], tpl[2])
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \n' + ''.join(s))
        sys.stdout.flush()
        result = (0, 0, "EQ_ABORT")
    return result


def IN_get(eq_type, delay=0.5, timeout=2.0):
    """ returns (eq_id, json_out, json_in) or None on timeout """
    try:
        tpl = queue_pop("emews_queue_IN", eq_type, delay, timeout)
        result = (tpl[0], tpl[2], tpl[3])
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()
        result = (0, None, "EQ_ABORT")
    return result


def done(msg):
    if msg == "EQ_FINAL":
        return True
    if msg == "EQ_ABORT":
        print("eq.done(): WARNING: EQ_ABORT")
        return True
    return False
