
# R TEST 1 WF

suppressMessages(library(EQ.SQL))

print("R TEST 1 WF: START")

if (! eq_init()) {
  quit(status = 1)
}

while (TRUE) {
  msg <- eq_query_task(eq_type = 0)
  params <- msg$payload
  if (params == EQ_STOP | params == EQ_ABORT) {
    break
  }
}

print("R TEST 1 WF: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
