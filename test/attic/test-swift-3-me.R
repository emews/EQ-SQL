
# TEST SWIFT 3 ME R

print("TEST SWIFT 3 ME R: START")

library(EQ.SQL)

if (! eq.init()) {
  quit(status=1)
}

for (i in seq(3)) {
  eq.OUT_put(eq_type=0, "0.5,0.5")
  results <- eq.IN_get(eq_type=0)
  cat("results: ", results, "\n")
}

eq.OUT_put(eq_type=0, "EQ_STOP")

print("TEST SWIFT 3 ME: STOP")
