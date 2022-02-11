
# PY TEST 3 WF

import json

import eq


print("PY TEST 3 WF: START")

eq.init()

def recv(eq_type):
    msg = eq.OUT_get(eq_type)
    try:
        eq_id = int(msg)
    except:
        return 'EQ_ABORT'
    print("eq_id=%i" % eq_id)
    params = eq.DB_json_out(eq_id)
    print(params)
    return (eq_id, params)

while True:
    eq_task_id, payload_str = recv(0)
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
