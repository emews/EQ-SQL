
# TASKJ PY
# Trig task that handles JSON

import json, sys
from math import sin


def f(params):
    print("JSON: '%s'" % params)
    sys.stdout.flush()
    J = json.loads(params)  # E.g. {values: [x,y]}
    V = J["values"]
    x = V["x"]
    y = V["y"]
    result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
    print("TASK: " + str(x) + " " + str(y) + " -> " + str(result))
    sys.stdout.flush()
    return result
