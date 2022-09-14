
# TASKJ PY
# Trig task that handles JSON

import json, sys
from math import sin

import eq.eq as eq


def f(eq_id):
    eq.init()
    print("f(%i) ..." % eq_id)
    sys.stdout.flush()
    json_out = eq.DB_json_out(eq_id)
    J = json.loads(json_out)  # E.g. {values: [x,y]}
    V = J["values"]
    x = V["x"]
    y = V["y"]
    result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
    print("TASK: " + str(x) + " " + str(y) + " -> " + str(result))
    sys.stdout.flush()
    kv = { "result": result }
    json_in = json.dumps(kv)
    eq.DB_result(eq_id, json_in)
    print("RESULT: eq_id=%i -> '%s'" % (eq_id, json_in))
    return eq_id
