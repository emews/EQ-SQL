suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 2 ME: START")

if (! eq.init()) {
  quit(status=1)
}

for (i in seq(1,3)) {
    payload <- sprintf('{"p": %i}', i)
    # print(paste0('Payload: ', payload))
    eq_task_id <- eq.submit.task('r_test_2', eq_type=0, payload=payload)
    res <- eq.IN_get(eq_task_id, timeout=5.0)
    if (eq.done(res[[2]])) break
    result_str <- DB.json.in(eq_task_id)
    r_list = fromJSON(result_str)
    # print(result)
    stopifnot(r_list$result == i)
}

f_task_id <- eq.DB.final()

print("R TEST 2 ME: STOP")