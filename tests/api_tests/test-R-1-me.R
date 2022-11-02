
# R TEST 1 ME

suppressMessages(library(EQ.SQL))

print("R TEST 1 ME: START")

if (! eq_init(log_level=logger::INFO)) {
  quit(status=1)
}

for (i in seq(1:3)) {
  result <- eq_submit_task('test_r_1', 0, paste("{params:", i, "}"))
}
res <- eq_stop_worker_pool(0)

print("R TEST 1 ME: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
