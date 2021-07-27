
# TEST SWIFT 2 ME R

print("TEST SWIFT 2 ME R: START")

library(EQ.SQL)

if (! eq.init()) {
  quit(status=1)
}

for (i in seq(3)) {
  eq.OUT_put("0.5,0.5")
  results <- eq.IN_get()
  cat("results: ", results, "\n")
}

eq.OUT_put("EQ_FINAL")

print("TEST SWIFT 2 ME: STOP")
