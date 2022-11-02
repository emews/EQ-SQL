#!/usr/bin/env python3

# CHECK QUEUES PY
# Check that the EMEWS Queues are empty before running a test

from eqsql import eq

eq.init()

success = True

with eq._DB.conn:
    with eq._DB.conn.cursor() as cur:
        tables = ["emews_queue_in", "emews_queue_out"]
        for table in tables:
            cur.execute(f"select count(eq_task_id) from {table};")
            rs = cur.fetchone()
            count = rs[0]
            if count > 0:
                print(f"check-queues.py: There are entries in table '{table}'")
                success = False

        cur.execute("select count(eq_task_id) from eq_tasks;")
        rs = cur.fetchone()
        count = rs[0]
        if count > 0:
            print(f"check-queues.py: There are entries in table 'eq_tasks'")
            success = True

if not success:
    exit(1)

eq.close()
