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
    result <- eq.query.result(eq_task_id)
    if (result[[1]] != ResultStatus$SUCCESS) {
        print(result)
        break
    }
    r_list = fromJSON(result[[2]])
    # print(result)
    stopifnot(r_list$result == i)
}

f_task_id <- eq.stop.worker.pool(0)

print("R TEST 2 ME: STOP")