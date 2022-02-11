
# PY TEST 2 WF

import json

import eq


print("PY TEST 2 WF: START")

eq.init()

def recv(eq_type):
    msg = eq.OUT_get(eq_type)
    try:
        eq_id = int(msg)
    except:
        return 'EQ_ABORT'
    print("eq_id=%i" % eq_id)
    params = eq.DB_json_out(eq_id)
    return (eq_id, params)

while True:
    eq_task_id, params = recv(0)
    if eq.done(params):
        break
    J = json.loads(params)
    print("J: %s" % str(J))
    value = J["params"]
    result = '{"result":%s}' % value
    eq.DB_result(eq_task_id, result)
    eq.IN_put(0, eq_task_id)

print("PY TEST 2 WF: STOP")
