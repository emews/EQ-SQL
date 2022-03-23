
# R TEST 1 ME

library(EQ.SQL)

print("R TEST 1 ME: START")

if (! eq.init()) {
  quit(status=1)
}

task_id <- eq.submit.task('test_py_1', 0, "{params: 42)")
print(paste0('task_id: ', task_id))

print("R TEST 1 ME: STOP")

# Local Variables:
# mode: R;
# eval: (setq ess-default-style 'DEFAULT)
# eval: (setq ess-indent-level 2)
# End:
