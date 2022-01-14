
# PY TEST 2 ME

import eq

print("PY TEST 2 ME: START")

eq.init()

for i in range(0, 3):
    # eq.OUT_put(eq_type=0, params="message:%i" % i)
    eq.DB_submit(eq_type=0, payload='{"params":%i}' % i)
    msg = eq.IN_get(eq_type=0)
    print("ME: msg=%s" % str(msg))
    assert msg is not None
    result = msg[2]
    assert result is not None
    assert result == '{"result":%i}' % i, "result='%s'" % str(result)
eq.DB_final(eq_type=0)

print("PY TEST 2 ME: STOP")
