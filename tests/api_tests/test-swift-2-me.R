
# TEST SWIFT 3 ME R

print("TEST SWIFT 2 ME R: START")
library(EQ.SQL)
library(jsonlite)

venv <- '~/.venv/py3.10/bin/python3'
eqpath <- '~/Documents/repos/EQ-SQL/python'

setup_task_queue <- function(eqsql) {
  host = Sys.getenv('DB_HOST')
  user = Sys.getenv('DB_USER')
  port = as.integer(Sys.getenv('DB_PORT'))
  db_name = Sys.getenv('DB_NAME')
  
  task_queue <- NULL
  task_queue <- init_task_queue(eqsql, host, user, port, db_name)
  
  eq_utils <- import_from_path('utils', path='/home/nick/Documents/repos/EQ-SQL/python/test')
  eq_utils$clear_db(task_queue$db$conn)
  task_queue
}

eqsql <- init_eqsql(python_path = venv, eqsql_path = eqpath)
task_queue <- setup_task_queue(eqsql)

SIM_WORK_TYPE <- 1

for (i in seq(3)) {
  params <- list(list(x=c(1.1 + i), y=4.0), list(x=2.1 + i, y=3.5))
  payload <- toJSON(params, auto_unbox=T)
  ft <- task_queue$submit_task('r-swift-2', eq_type=SIM_WORK_TYPE, payload=payload)[[2]]
  result <- ft$result()
  if (result[[1]] != eqsql$eq$ResultStatus$SUCCESS) {
    print(result)
    break
  }
  cat("results: ", result[[2]], "\n")
}

task_queue$stop_worker_pool(1)
print("TEST SWIFT 2 ME R: STOP")
