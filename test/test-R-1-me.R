
# R TEST 1 ME

library(EQ.SQL)

print("R TEST 1 ME: START")

if (! eq.init()) {
  quit(status=1)
}

## for (i in seq(3)) {
##   eq.OUT_put(sprintf("message:%i", i))
## }
## eq.OUT_put("EQ_FINAL")

eq.OUT_put(sprintf("message:%i", 42))

## print("PY TEST 1 ME: STOP")

# Local Variables:
# (setq ess-indent-offset 2)
# End:
