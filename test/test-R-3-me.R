suppressMessages(library(EQ.SQL))
library(jsonlite)

print("R TEST 3 ME: START")

if (! eq.init()) {
  quit(status=1)
}

for (i in seq(3)) {
    params <- list(list(param=0), list(param=1), list(param=2))
    payload <- toJSON(params, auto_unbox=T)
    eq_task_id <- eq.submit.task('r-test-3', eq_type=0, payload=payload)
    result <- eq.query.result(eq_task_id)
    if (result[[1]] != ResultStatus$SUCCESS) {
        print(result)
        break
    }
    r_list = fromJSON(result[[2]])
    stopifnot(all(r_list == c(0, 1, 2)))
}

f_task_id <- eq.stop.worker.pool(0)
print("R TEST 3 ME: STOP")