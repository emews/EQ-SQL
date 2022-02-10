
# PY TEST 1 WF

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    tpl = eq.OUT_get(eq_type=0)
    print("tuple: " + str(tpl))
    eq_id, eq_type, msg = tpl
    if msg == "EQ_FINAL":
        break
    if msg == "EQ_ABORT":
        break

print("PY TEST 1 WF: STOP")
