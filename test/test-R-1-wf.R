
# R TEST 1 WF

library(EQ.SQL)

print("R TEST 1 WF: START")

if (! eq.init()) {
  quit(status=1)
}

msg <- eq.OUT_get(eq_type=0, delay=1, timeout=3)

cat("msg: ", msg, "\n")

print("R TEST 1 WF: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
