
# PY TEST 1 ME

import eq


print("PY TEST 1 ME: START")

eq.init()

for i in range(0, 3):
    eq.OUT_put("message:%i" % i)
eq.OUT_put("EQ_FINAL")

print("PY TEST 1 ME: STOP")
