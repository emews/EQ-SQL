
# R TEST 1 WF

library(EQ.SQL)

print("R TEST 1 WF: START")

if (! eq.init()) {
  quit(status=1)
}

msg <- eq.OUT_get(type=0, delay=1, timeout=3)

cat("msg: ", msg, "\n")

print("R TEST 1 WF: STOP")
