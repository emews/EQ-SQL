
# PY TEST 2 ME

import eq

print("PY TEST 2 ME: START")

eq.init()

for i in range(0, 3):
    eq.OUT_put(eq_type=0, params="message:%i" % i)
    result = eq.IN_get(eq_type=0)
    assert result == "result:%i" % i, "result="+result
eq.OUT_put(eq_type=0, params="EQ_FINAL")

print("PY TEST 2 ME: STOP")
