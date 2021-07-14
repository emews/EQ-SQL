
# PY TEST 2 ME

import eq

print("PY TEST 2 ME: START")

eq.init()

for i in range(0, 3):
    eq.OUT_put("message:%i" % i)
    result = eq.IN_get()
    assert result == "result:%i" % i, "result="+result

print("PY TEST 2 ME: STOP")
