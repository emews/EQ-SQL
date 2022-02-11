
# PY TEST 3 ME

import eq
import json

print("PY TEST 3 ME: START")

eq.init()

for i in range(0, 3):
    payload = [{'param': i} for i in range(3)]
    # TODO: include exp_id as arg
    eq_id = eq.DB_submit(eq_type=0, payload=json.dumps(payload))
    eq.OUT_put(0, eq_id)
    # TODO: wait for eq_id finished, not eq_type
    msg = eq.IN_get(eq_type=0)
    print("ME: msg=%s" % str(msg))
    if eq.done(msg): break
    eq_id = int(msg)
    value = eq.DB_json_in(eq_id)
    # assert value == '{"result":%i}' % i, "msg='%s'" % str(msg)
    assert value == json.dumps([0, 1, 2])
    print(f'VALUE: {value}')
eq.DB_final()

print("PY TEST 3 ME: STOP")
