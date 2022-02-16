
# PY TEST 2 ME

import eq

print("PY TEST 2 ME: START")

eq.init()

for i in range(0, 3):

    # eq_task_id = eq.DB_submit('py_test_2', eq_type=0, payload='{"params":%i}' % i)
    # eq.OUT_put(0, eq_task_id)
    eq_task_id = eq.sumbit_task('py_test_2', eq_type=0, payload='{"params":%i}' % i)
    msg = eq.IN_get(eq_task_id)
    print("ME: msg=%s" % str(msg))
    if eq.done(msg): break
    value = eq.DB_json_in(eq_task_id)
    assert value == '{"result":%i}' % i, "msg='%s'" % str(msg)
eq.DB_final()

print("PY TEST 2 ME: STOP")
