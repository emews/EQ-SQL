
# PY TEST 2 WF

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    msg = eq.OUT_get()
    if msg is None:
        print("queue is empty")
        break
    print("msg: " + str(msg))
    if msg == "EQ_FINAL":
        break
    tokens = msg.split(":")
    result = "result:" + tokens[1]
    eq.IN_put(result)

print("PY TEST 2 WF: STOP")
