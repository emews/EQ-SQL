
# PY TEST 1 ME

# import json

import eq


print("PY TEST 1 ME: START")

eq.init()

for i in range(0, 3):
    eq_id = eq.DB_submit("test_py_1", 0, "{message:%i}" % i)
    eq.OUT_put(0, eq_id)
eq.DB_final()

print("PY TEST 1 ME: STOP")
