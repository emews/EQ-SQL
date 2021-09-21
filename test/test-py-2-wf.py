
# PY TEST 2 WF

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    msg = eq.OUT_get(eq_type=0)
    if msg is None:
        print("queue is empty")
        break
    print("msg: " + str(msg))
    if eq.done(msg):
        break
    tokens = msg.split(":")
    result = "result:" + tokens[1]
    eq.IN_put(eq_type=0, params=result)

print("PY TEST 2 WF: STOP")
