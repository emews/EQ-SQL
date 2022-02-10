
# PY TEST 2 WF

import json

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    tpl = eq.OUT_get(eq_type=0)
    print("tpl: " + str(tpl))
    if tpl is None:
        print("queue is empty")
        break
    eq_id, eq_type, msg = tpl
    if eq.done(msg):
        break
    print("msg: " + str(msg))
    value = json.loads(msg)
    print("value: %s" % str(value))
    value = value["params"]
    result = '{"result":%s}' % value
    eq.DB_result(eq_id, result)

print("PY TEST 2 WF: STOP")
