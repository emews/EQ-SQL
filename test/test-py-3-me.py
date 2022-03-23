
# PY TEST 3 ME

import eq
import json

print("PY TEST 3 ME: START")

eq.init()

for i in range(0, 3):
    payload = [{'param': i} for i in range(3)]
    eq_task_id = eq.sumbit_task('py_test_3', eq_type=0, payload=json.dumps(payload))
    msg = eq.IN_get(eq_task_id)
    print("ME: msg=%s" % str(msg))
    if eq.done(msg[1]): break
    value = eq.DB_json_in(eq_task_id)
    assert value == json.dumps([0, 1, 2])
    print(f'VALUE: {value}')
eq.DB_final(0)

print("PY TEST 3 ME: STOP")
