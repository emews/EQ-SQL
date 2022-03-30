suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 3 ME: START")

if (! eq_init()) {
  quit(status=1)
}

for (i in seq(3)) {
    params <- list(list(param=0), list(param=1), list(param=2))
    payload <- toJSON(params, auto_unbox=T)
    eq_task_id <- eq_submit_task('r-test-3', eq_type=0, payload=payload)
    result <- eq_query_result(eq_task_id)
    if (result[[1]] != ResultStatus$SUCCESS) {
        print(result)
        break
    }
    r_list = fromJSON(result[[2]])
    stopifnot(all(r_list == c(0, 1, 2)))
}

f_task_id <- eq_stop_worker_pool(0)
print("R TEST 3 ME: STOP")