
# PY TEST 3 WF

import json

import eq.eq as eq


print("PY TEST 3 WF: START")

eq.init()


while True:
    msg_map = eq.query_task(eq_type=0)
    payload_str = msg_map['payload']
    if msg_map['type'] != 'work':
        break
    payload = json.loads(payload_str)
    print(f'payload: {payload}')

    result = []
    for p in payload:
        value = p["param"]
        result.append(value)
        # result = '{"result":%s}' % value

    eq.report_task(msg_map['eq_task_id'], 0, json.dumps(result))

print("PY TEST 3 WF: STOP")
