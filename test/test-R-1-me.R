
# R TEST 1 ME

library(EQ.SQL)

print("R TEST 1 ME: START")

eq.init()

for (i in seq(3)) {
  eq.OUT_put(sprintf("message:%i", i))
}
eq.OUT_put("EQ_FINAL")

## print("PY TEST 1 ME: STOP")

# Local Variables:
# (setq ess-indent-offset 2)
# End:
