
# TEST SWIFT 2 ME PY

import os
import eq.eq as eq
import algorithm

print("TEST SWIFT 2 ME: START")

EQ_SQL = os.getenv("EQ_SQL")

eq.init()

algorithm.load_settings(EQ_SQL + "/swift/settings_w_proxy.json")
algorithm.run()

print("TEST SWIFT 2 ME: STOP")
