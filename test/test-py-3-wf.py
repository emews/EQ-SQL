
# PY TEST 3 WF

import json

import eq


print("PY TEST 3 WF: START")

eq.init()


while True:
    eq_task_id, payload_str = eq.query_task(eq_type=0)
    if eq.done(payload_str):
        break
    payload = json.loads(payload_str)
    print(f'payload: {payload}')

    result = []
    for p in payload:
        value = p["param"]
        result.append(value)
        # result = '{"result":%s}' % value

    eq.report_task(0, eq_task_id, json.dumps(result))

print("PY TEST 3 WF: STOP")
