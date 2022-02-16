
# PY TEST 2 WF

import json

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    eq_task_id, params = eq.query_task(0)
    if eq.done(params):
        break
    J = json.loads(params)
    print("J: %s" % str(J))
    value = J["params"]
    result = '{"result":%s}' % value
    eq.report_task(0, eq_task_id, result)

print("PY TEST 2 WF: STOP")
