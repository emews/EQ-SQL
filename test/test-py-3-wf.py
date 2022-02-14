
# PY TEST 3 WF

import json

import eq


print("PY TEST 3 WF: START")

eq.init()


while True:
    eq_task_id, payload_str = eq.query_work(0)
    if eq.done(payload_str):
        break
    payload = json.loads(payload_str)
    print(f'payload: {payload}')

    result = []
    for p in payload:
        value = p["param"]
        result.append(value)
        # result = '{"result":%s}' % value

    eq.DB_result(eq_task_id, json.dumps(result))
    eq.IN_put(0, eq_task_id)

print("PY TEST 3 WF: STOP")
