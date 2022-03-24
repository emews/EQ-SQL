
# TEST SWIFT 3 ME R

print("TEST SWIFT 2 ME R: START")
library(EQ.SQL)
library(jsonlite)

if (! eq.init()) {
  quit(status=1)
}

SIM_WORK_TYPE <- 1

for (i in seq(3)) {
  params <- list(list(x=c(1.1 + i), y=4.0), list(x=2.1 + i, y=3.5))
  payload <- toJSON(params, auto_unbox=T)
  eq_task_id <- eq.submit.task('r-swift-2', eq_type=SIM_WORK_TYPE, payload=payload)
  result <- eq.query.result(eq_task_id)
  if (result[[1]] != ResultStatus$SUCCESS) {
    print(result)
    break
  }
  cat("results: ", result[[2]], "\n")
}

eq.DB.final(1)
print("TEST SWIFT 2 ME R: STOP")
