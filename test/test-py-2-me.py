
# PY TEST 2 ME

import eq

print("PY TEST 2 ME: START")

eq.init()

for i in range(0, 3):
    # eq.OUT_put(eq_type=0, params="message:%i" % i)
    eq.DB_submit(eq_type=0, payload='{"params":%i}' % i)
    tpl = eq.IN_get(eq_type=0)
    print("ME: tpl=%s" % str(tpl))
    assert tpl is not None
    eq_id, eq_type, msg = tpl
    assert msg == '{"result":%i}' % i, "msg='%s'" % str(msg)
eq.DB_final()

print("PY TEST 2 ME: STOP")
