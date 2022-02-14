
# PY TEST 1 WF

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    eq_task_id, params = eq.query_work(eq_type=0)
    if params == "EQ_FINAL":
        break
    if params == "EQ_ABORT":
        break
    print("params: " + str(params))


print("PY TEST 1 WF: STOP")
