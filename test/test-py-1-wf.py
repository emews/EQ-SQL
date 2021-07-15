
# PY TEST 1 ME

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    msg = eq.OUT_get()
    print("msg: " + str(msg))
    if msg == "EQ_FINAL":
        break

print("PY TEST 1 WF: STOP")
