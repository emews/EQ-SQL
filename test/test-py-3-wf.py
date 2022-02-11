
# PY TEST 3 WF

import json

import eq


print("PY TEST 3 WF: START")

eq.init()

while True:
    msg = eq.OUT_get(eq_type=0)
    print("msg: " + str(msg))
    if eq.done(msg):
        break
    eq_id = int(msg)
    payload_str = eq.DB_json_out(eq_id)
    payload = json.loads(payload_str)
    print(f'payload: {payload}')

    result = []
    for p in payload:
        value = p["param"]
        result.append(value)
        # result = '{"result":%s}' % value

    eq.DB_result(eq_id, json.dumps(result))
    eq.IN_put(0, str(eq_id))

print("PY TEST 3 WF: STOP")
