
# PY TEST 1 ME

# import json

import eq


print("PY TEST 1 ME: START")

eq.init()

for i in range(0, 3):
    eq.DB_submit(0, "{message:%i}" % i)
eq.DB_stop()

print("PY TEST 1 ME: STOP")
