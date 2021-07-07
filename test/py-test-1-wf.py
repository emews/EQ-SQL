
# PY TEST 1 ME

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    msg = eq.OUT_get()
    if msg is None:
        print("queue is empty")
        break
    print("msg: " + str(msg))

print("PY TEST 1 WF: STOP")
