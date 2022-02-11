
# EQ-SQL eq.py

import random
import sys
import threading
import traceback
import time
from datetime import datetime

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
    return DB


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


def sql_pop_q(table, eq_type):
    """
    Generate code for a queue pop from given table
    From:
    https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    Can only select 1 column from the subquery,
    but we return * from the deleted row.
    See workflow.sql for the returned queue row
    """
    code = """
    DELETE FROM %s
    WHERE  eq_task_id = (
    SELECT eq_task_id
    FROM %s
    WHERE eq_type = %i
    ORDER BY eq_task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    """ % (table, table, eq_type)
    return code


def queue_pop(table, eq_type, delay, timeout):
    """
    returns eq_task_id
    or None on timeout
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
    if rs is None: return None  # timeout
    return rs[1]


# def queue_push(table, eq_type, eq_task_id, priority):
#     DB.insert(table, ["eq_type",  "eq_task_id", "eq_priority"],
#                      [ eq_type, eq_task_id, priority])


def DB_submit(exp_id, eq_type, payload):
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_task_id = rs[0]
    DB.insert("eq_tasks", ["eq_task_id", "eq_type", "json_out", "time_created"],
              [ eq_task_id , eq_type, Q(payload), Q(str(datetime.now()))])
    DB.insert("eq_exp_id_tasks", ["exp_id", "eq_task_id"],
              [Q(exp_id),  eq_task_id])
    return eq_task_id


def DB_json_out(eq_task_id):
    """ return the json_out for the int eq_task_id """
    global DB
    print("DB_json_out")
    sys.stdout.flush()
    DB.select("eq_tasks", "json_out", f'eq_task_id={eq_task_id}')
    rs = DB.get()
    result = rs[0]
    return result


def DB_json_in(eq_task_id):
    """ return the json_in for the int eq_task_id """
    global DB
    print("DB_json_out")
    sys.stdout.flush()
    DB.select("eq_tasks", "json_in", f'eq_task_id={eq_task_id}')
    rs = DB.get()
    result = rs[0]
    return result


def DB_result(eq_task_id, payload):
    global DB
    print("DB_result:")
    sys.stdout.flush()
    DB.update("eq_tasks", ["json_in"], [Q(payload)],
                              where=f'eq_task_id={eq_task_id}')


def DB_final():
    global DB
    DB.execute("select nextval('emews_id_generator');")
    rs = DB.get()
    eq_task_id = rs[0]
    DB.insert("eq_tasks", ["eq_task_id", "eq_type", "json_out"],
                              [ eq_task_id , 0, Q("EQ_FINAL")])
    OUT_put(0, eq_task_id)
    return eq_task_id


def OUT_put(eq_type, eq_task_id, priority=0):
    """"""
    try:
        # queue_push("emews_queue_OUT", eq_type, eq_task_id, priority)
        DB.insert('emews_queue_OUT', ["eq_type",  "eq_task_id", "eq_priority"],
                     [ eq_type, eq_task_id, priority])

    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()
    return eq_task_id


def IN_put(eq_type, eq_task_id):
    try:
        DB.insert('emews_queue_IN', ["eq_type",  "eq_task_id"],
                     [ eq_type, eq_task_id])

        #queue_push("emews_queue_IN", eq_type, eq_task_id)
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \\n' + ''.join(s))
        sys.stdout.flush()


def OUT_get(eq_type, delay=0.5, timeout=2.0):
    """
    returns eq_task_id
    on timeout or error: returns 'EQ_ABORT'
    """
    try:
        print("OUT_get():")
        sys.stdout.flush()
        result = queue_pop("emews_queue_OUT", eq_type, delay, timeout)
        if result is None:
            print("eq.py:OUT_get(eq_type=%i): popped None: abort!" %
                  eq_type)
            sys.stdout.flush()
            result = "EQ_ABORT"
    except Exception as e:
        info = sys.exc_info()
        s = traceback.format_tb(info[2])
        print(str(e) + ' ... \n' + ''.join(s))
        sys.stdout.flush()
        result = "EQ_ABORT"
    return result


# def out_get_payload(eq_type):
#     """ WIP Simplified wrapper for Swift/T """
#     tpl = OUT_get(eq_type)
#     result = str(tpl[0])
#     print("out_get_payload(): result: " + result)
#     return result

# TODO: Updated to work with eq_task_id NOT eq_type
def IN_get(eq_task_id, delay=0.5, timeout=2.0):
    """
    returns: eq_task_id
    on timeout or error: returns EQ_ABORT
    """
    try:
        result = queue_pop("emews_queue_IN", eq_type, delay, timeout)
        if result is None:
            print("eq.py:IN_get(eq_type=%i): popped None: abort!" %
                  eq_type)
            sys.stdout.flush()
            result = "EQ_ABORT"
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
