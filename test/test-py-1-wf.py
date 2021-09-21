
# PY TEST 1 WF

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    msg = eq.OUT_get(eq_type=0)
    print("msg: " + str(msg))
    if msg == "EQ_FINAL":
        break
    if msg == "EQ_ABORT":
        break

print("PY TEST 1 WF: STOP")
