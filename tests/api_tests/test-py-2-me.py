
# PY TEST 2 ME

import eq.eq as eq

print("PY TEST 2 ME: START")

eq.init()

for i in range(0, 3):
    status, eq_task_id = eq.submit_task('py_test_2', eq_type=0, payload='{"params":%i}' % i)
    result = eq.query_result(eq_task_id)
    if result[0] != eq.ResultStatus.SUCCESS:
        print(result, flush=True)
        break

    assert result[1] == '{"result":%i}' % i, "msg='%s'" % str(result)
eq.stop_worker_pool(0)

print("PY TEST 2 ME: STOP")
