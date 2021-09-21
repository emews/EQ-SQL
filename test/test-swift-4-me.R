
# TEST SWIFT 4 ME R

print("TEST SWIFT 4 ME R: START")

library(rjson)
library(EQ.SQL)

if (! eq.init()) {
  quit(status=1)
}

for (i in seq(3)) {
  x = i * 0.1
  y = i * 0.01
  # See taskj.py for JSON structure
  L <- list(x=x, y=y)
  D <- list(values=L)
  J = toJSON(D)
  cat("R toJSON: ", J, "\n")
  eq.OUT_put(J)
  results <- eq.IN_get()
  cat("results: ", results, "\n")
}

eq.OUT_put("EQ_FINAL")

print("TEST SWIFT 4 ME: STOP")
