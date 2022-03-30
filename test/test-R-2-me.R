suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 2 ME: START")

if (! eq_init()) {
  quit(status=1)
}

for (i in seq(1,3)) {
    payload <- sprintf('{"p": %i}', i)
    # print(paste0('Payload: ', payload))
    eq_task_id <- eq_submit_task('r_test_2', eq_type=0, payload=payload)
    result <- eq_query_result(eq_task_id)
    if (result[[1]] != ResultStatus$SUCCESS) {
        print(result)
        break
    }
    r_list = fromJSON(result[[2]])
    # print(result)
    stopifnot(r_list$result == i)
}

f_task_id <- eq_stop_worker_pool(0)

print("R TEST 2 ME: STOP")