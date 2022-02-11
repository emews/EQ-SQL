
# PY TEST 1 WF

import eq


print("PY TEST 1 WF: START")

eq.init()

def recv(eq_type):
    msg = eq.OUT_get(eq_type=0)
    try:
        eq_id = int(msg)
    except:
        return 'EQ_ABORT'
    print("eq_id=%i" % eq_id)
    params = eq.DB_json_out(eq_id)
    return params


while True:
    params = recv(eq_type=0)
    if params == "EQ_FINAL":
        break
    if params == "EQ_ABORT":
        break
    print("params: " + str(params))


print("PY TEST 1 WF: STOP")
