#!/usr/bin/env python3

# CHECK QUEUES PY
# Check that the EMEWS Queues are empty before running a test

from db_tools import workflow_sql

sql = workflow_sql(envs=True)
sql.connect()

success = True

tables = [ "emews_queue_IN", "emews_queue_OUT" ]
for table in tables:
    sql.select(table=table, what="count(eq_task_id)")
    rs = sql.get()
    count = rs[0]
    if count > 0:
        print("check-queues.py: There are entries in table '%s'" %
              table)
        success = False

tables = [ "eq_tasks" ]
for table in tables:
    sql.select(table=table, what="count(eq_task_id)")
    rs = sql.get()
    count = rs[0]
    if count > 0:
        print("check-queues.py: There are entries in table '%s'" %
              table)
        success = True

if not success:
    exit(1)
