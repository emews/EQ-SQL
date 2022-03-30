
# PY TEST 2 WF

import json

import eq


print("PY TEST 2 WF: START")

eq.init()

while True:
    msg_map = eq.query_task(0)
    payload = msg_map['payload']
    if msg_map['type'] != 'work':
        break
    J = json.loads(payload)
    print("J: %s" % str(J))
    value = J["params"]
    result = '{"result":%s}' % value
    eq.report_task(msg_map['eq_task_id'], 0, result)

print("PY TEST 2 WF: STOP")
