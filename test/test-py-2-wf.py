
# PY TEST 2 WF

import json

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    msg = eq.OUT_get(eq_type=0)
    if msg is None:
        print("queue is empty")
        break
    print("msg: " + str(msg))
    eq_id, payload = msg
    if eq.done(payload):
        break
    value = json.loads(payload)
    print("value: %s" % str(value))
    value = value["params"]
    result = '{"result":%s}' % value
    eq.DB_result(eq_id, result)

print("PY TEST 2 WF: STOP")
