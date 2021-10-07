
# R TEST 1 ME

library(EQ.SQL)

print("R TEST 1 ME: START")

if (! eq.init()) {
  quit(status=1)
}

eq.OUT_put(eq_type=0, sprintf("message:%i", 42))

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
