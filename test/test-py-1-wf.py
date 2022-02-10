
# PY TEST 1 WF

import eq


print("PY TEST 1 WF: START")

eq.init()

while True:
    pair = eq.OUT_get(eq_type=0)
    print("pair: " + str(pair))
    eq_id, msg = pair
    if msg == "EQ_FINAL":
        break
    if msg == "EQ_ABORT":
        break

print("PY TEST 1 WF: STOP")
