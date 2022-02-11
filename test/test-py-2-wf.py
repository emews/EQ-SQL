
# PY TEST 2 WF

import json

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    msg = eq.OUT_get(eq_type=0)
    print("msg: " + str(msg))
    if eq.done(msg):
        break
    eq_id = int(msg)
    params = eq.DB_json_out(eq_id)
    J = json.loads(params)
    print("J: %s" % str(J))
    value = J["params"]
    result = '{"result":%s}' % value
    eq.DB_result(eq_id, result)
    eq.IN_put(0, str(eq_id))

print("PY TEST 2 WF: STOP")
