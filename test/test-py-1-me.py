
# PY TEST 1 ME

# import json

import eq.eq as eq


print("PY TEST 1 ME: START")

eq.init()

for i in range(0, 3):
    status, eq_task_id = eq.submit_task('test_py_1', 0, "{message:%i}" % i)
eq.stop_worker_pool(0)

print("PY TEST 1 ME: STOP")
