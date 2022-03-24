
# PY TEST 3 ME

import eq
import json

print("PY TEST 3 ME: START")

eq.init()

for i in range(0, 3):
    payload = [{'param': i} for i in range(3)]
    eq_task_id = eq.submit_task('py_test_3', eq_type=0, payload=json.dumps(payload))
    result = eq.query_result(eq_task_id)
    if result[0] != eq.ResultStatus.SUCCESS:
        print(result, flush=True)
        break

    assert result[1] == json.dumps([0, 1, 2])
    print(f'VALUE: {result[1]}')

eq.DB_final(0)

print("PY TEST 3 ME: STOP")
