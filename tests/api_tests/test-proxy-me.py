
# TEST SWIFT 2 ME PY

import os
import algorithm

print("TEST SWIFT 2 ME: START")

EQ_SQL = os.getenv("EQ_SQL")

algorithm.load_settings(EQ_SQL + "/tests/api_tests/settings_proxy.json")
algorithm.run()

print("TEST SWIFT 2 ME: STOP")
