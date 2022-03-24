
# PY TEST 1 WF

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    msg_map = eq.query_task(eq_type=0)
    params = msg_map['payload']
    if params == "EQ_STOP":
        break
    if params == "EQ_ABORT":
        break
    print("params: " + str(params))


print("PY TEST 1 WF: STOP")
