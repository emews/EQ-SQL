
# R TEST 1 WF

library(EQ.SQL)

print("R TEST 1 WF: START")

if (! eq.init()) {
  quit(status=1)
}


msg <- eq.query.task(eq_type=0)
print(msg)

print("R TEST 1 WF: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
