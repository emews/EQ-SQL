
# TASKJ PY
# Trig task that handles JSON

import json, sys
from math import sin


def f(params):
    J = json.loads(params)
    x = J["x"]
    y = J["y"]
    result = sin(4*x)+sin(4*y)+-2*x+x**2-2*y+y**2
    print("TASK: " + str(x) + " " + str(y) + " -> " + str(result))
    sys.stdout.flush()
    return result
