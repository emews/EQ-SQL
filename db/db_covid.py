
# DB COVID PY
# Tools for the COVID/RepastHPC Workflow DB
# Typically called from db_covid.swift
# Uses db_tools.py

import logging
import os
import random
import time

from eqsql.db_tools import workflow_sql, workflow_sql_setup, DB_Mode, q, qA, \
                     ConnectionException

from enum import Enum, unique

@unique
class RunStatus(Enum):
    NULL    = 0   # undefined (SQL default)
    # PROTO   = 1 # reserved for future use
    START   = 2
    SUCCESS = 3
    ERROR   = 4


sql    = None
logger = None
rank   = None # string, for use as procname
delay  = None # DB backoff interval in seconds
exp_int = None # The experiment ID as integer

def init():
    """ Initialize this module, just the logger for now
        Must be called by every function:
        remember these functions can run on different ranks
    """
    global logger, rank, delay
    if logger == None:
        rank   = "%-4s" % os.getenv("ADLB_RANK_SELF")
        logger = logging.getLogger("DB_COVID:")
        handlr = logging.StreamHandler()
        formtr = logging.Formatter("%(asctime)s " + rank +
                                   " %(name)-9s %(message)s",
                                   datefmt='%Y-%m-%d %H:%M:%S')
        handlr.setFormatter(formtr)
        logger.addHandler(handlr)
        logger.setLevel(logging.DEBUG)
        # This works well on Theta: scale=1 causes many retries
        scale = 8
        # Do not let delay be 0- will never increase by multiplication
        delay = (scale * float(rank) + 1) / 1000.0
        random.seed(int(rank)) # for the delay backoff


def connect(expid=None):
    """ On first call, use expid to set global exp_int """
    global sql, logger, rank, delay, exp_int
    init()
    sql = workflow_sql_setup(sql, envs=True, log=True, procname=rank)
    if sql.mode >= DB_Mode.SOFT:
        logger.info("connect(): connecting ... delay: %4.1f" % delay)
        while True: # until success
            try:
                time.sleep(delay)
                result = sql.connect()
                delay = delay * 0.9
                break # success
            except ConnectionException as e:
                delay = delay * 4 * random.random()
                logger.debug("connect(): backoff: delay: %4.1f" % delay)
                logger.debug(str(e))
        if exp_int == None:
            sql.select("expids", "*", "expid='%s'" % expid);
            rs = sql.cursor.fetchone()
            # logger.info("connect(): rs: " + str(rs))
            if rs == None:
                logger.warning("db_covid.py:connect(): " +
                               "Received bad RS for expid='%s'" %
                               expid)
                if sql.mode != DB_Mode.SOFT:
                    return "EXCEPTION"
            exp_int = rs[0]
        result = str(exp_int)
    return result


def instance_start(exp_int_in, instance, js_params):
    global sql, logger, exp_int
    exp_int = exp_int_in
    init()
    logger.info("instance_start(): " + str(instance))
    sql = workflow_sql_setup(sql, envs=True, log=True, procname=rank)
    result = connect()
    values = qA(exp_int, instance, RunStatus.START.value, js_params)
    values.append("now()")
    result = sql.insert(table="exp_instnces",
                        names=[   "exp_int", "instnce", "status",
                                  "json_in", "time_start" ],
                        values=values)
    sql.close()
    return "OK"


def instance_stop(exp_int_in, instance, js_results):
    global sql, logger, exp_int
    exp_int = exp_int_in
    init()
    print("instance_stop: " + str(instance))
    sql = workflow_sql_setup(sql, envs=True, log=True, procname=rank)
    result = connect()
    result = sql.update(table  = "exp_instnces",
                        names  = [ "status", "json_out", "time_stop" ],
                        values = [ RunStatus.SUCCESS.value, q(js_results), "now()" ],
                        search = "exp_int="+str(exp_int) + " and " +
                                 "instnce="+str(instance))
    result = "OK"
    sql.close()
    return result


def run_start(exp_int_in, instance, run):
    global sql, logger, exp_int
    exp_int = exp_int_in
    init()
    print("run_start: %i/%i" % (instance, run))
    sql = workflow_sql_setup(sql, envs=True, log=True, procname=rank)
    result = connect()
    status = RunStatus.START.value
    result = sql.insert("exp_runs",
                 [   "exp_int", "instnce", "run", "status", "time_start" ],
                  qA( exp_int,   instance,  run,   status,  "now()"))
    result = "OK"
    sql.close()
    return result


def run_stop(exp_int_in, instance, run, js_results):
    global sql, logger, exp_int
    exp_int = exp_int_in
    init()
    print("run_stop: %i/%i" % (instance, run))
    sql = workflow_sql_setup(sql, envs=True, log=True, procname=rank)
    result = connect()
    status = RunStatus.SUCCESS.value
    result = sql.update(table  = "exp_runs",
                        names  = [ "status", "json_out",   "time_stop"],
                        values = [  status, q(js_results), "now()"],
                        search = "exp_int="+str(exp_int)  + " and " +
                                 "instnce="+str(instance) + " and " +
                                 "run="    +str(run))
    result = "OK"
    sql.close()
    return result


def get_instance_count():
    global sql, logger, exp_int
    sql.select("exp_instnces", "count(exp_int)", "exp_int=%i"%exp_int)
    rs = sql.cursor.fetchone()
    return rs[0]

def get_instances():
    global sql, logger, exp_int
    sql.select("exp_instnces", "*", "exp_int=%i"%exp_int)
    results = []
    while True:
        rs = sql.cursor.fetchone()
        if rs == None: break
        results.append(rs)
    return results

def get_runs(instance):
    global sql, logger, exp_int
    sql.select("exp_runs", "*", "exp_int=%i and instnce=%i" %
               (exp_int, instance))
    results = []
    while True:
        rs = sql.cursor.fetchone()
        if rs == None: break
        results.append(rs)
    return results

def maybe_string_int(s, field):
    if s is None:
        return "None"
    return field % int(s)
