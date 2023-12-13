library(reticulate)
library(jsonlite)
library(coro)
library(purrr)

setup_task_queue <- function(eqsql, env = parent.frame()) {
  host = 'localhost'
  user = 'eqsql_test_user'
  port = 5433L
  db_name = 'eqsql_test_db'
  
  task_queue <- NULL
  task_queue <- init_task_queue(eqsql, host, user, port, db_name)
  # api_url <- 'http://127.0.0.1:5000'
  # task_queue <- init_task_queue(eqsql, host, user, port, db_name, api_url)
  
  eq_utils <- import_from_path('utils', path='/home/nick/Documents/repos/EQ-SQL/python/test')
  eq_utils$clear_db(task_queue$db$conn)
  
  withr::defer({
    if (!is.null(task_queue)) {
      task_queue$close()
    }
  }, envir = env)

  task_queue
}

venv <- '~/.venv/py3.10/bin/python3'
eqpath <- '~/Documents/repos/EQ-SQL/python'

test_that("task_queue initialized", {
  expect_no_error(init_eqsql(python_path = venv, eqsql_path = eqpath))
})

test_that("task_queue initialized", {
  eqsql <- init_eqsql(python_path = venv, eqsql_path = eqpath)
  task_queue <- setup_task_queue(eqsql)
  expect_true(!is.null(task_queue$db))
})

create_payload <- function(x=1.2) {
  payload = list(x=x, y=7.3, z='foo')
  return (toJSON(payload, auto_unbox = T))
}

test_that("as_completed_n", {
  eqsql <- init_eqsql(python_path = venv, eqsql_path = eqpath)
  task_queue <- setup_task_queue(eqsql)
  
  # submit tasks
  fts <- lapply(c(1:50), function(x) {
      payload <- create_payload(x)
      sub_result <- task_queue$submit_task('eq_test', 0, payload)
      expect_equal(eqsql$task_queues$core$ResultStatus$SUCCESS, sub_result[[1]])
      sub_result[[2]]
  })
  
  expect_equal(50, length(fts))
  
  # complete tasks and post results
  lapply(c(1:50), function(x) {
    result <- task_queue$query_task(0, timeout = 0.0)
    task_id = result$eq_task_id
    task_result = list(j=task_id)
    task_queue$report_task(task_id, 0, toJSON(task_result, auto_unbox = T))
  })
  
  result <- as_completed(task_queue, fts, function(ft) {
    expect_true(ft$done())
    expect_equal(ft$eq_task_id, fromJSON(ft$result()[[2]])$j)
    ft$eq_task_id
   }, n = 10)
  
  expect_equal(length(result$fts), 50)
  completed_ids <- result$f_results
  expect_equal(length(completed_ids), 10)
  
  # remove completed from rest and run with no n
  # to get the rest of the completed futures
  fts <- discard(fts, function(ft) ft$eq_task_id %in% completed_ids)
  expect_equal(length(fts), 40)
  result <- as_completed(task_queue, fts, function(ft) {
    expect_true(ft$done())
    expect_equal(ft$eq_task_id, fromJSON(ft$result()[[2]])$j)
    ft$eq_task_id
  })

  completed_ids_2 <- result$f_results
  expect_equal(length(completed_ids_2), 40)
  expect_false(any(completed_ids %in% completed_ids_2))
})

test_that("as_completed_pop", {
  eqsql <- init_eqsql(python_path = venv, eqsql_path = eqpath)
  task_queue <- setup_task_queue(eqsql)
  
  # submit tasks
  fts <- lapply(c(1:50), function(x) {
    payload <- create_payload(x)
    sub_result <- task_queue$submit_task('eq_test', 0, payload)
    expect_equal(eqsql$task_queues$core$ResultStatus$SUCCESS, sub_result[[1]])
    sub_result[[2]]
  })
  
  expect_equal(50, length(fts))
  
  # complete tasks and post results
  lapply(c(1:50), function(x) {
    result <- task_queue$query_task(0, timeout = 0.0)
    task_id = result$eq_task_id
    task_result = list(j=task_id)
    task_queue$report_task(task_id, 0, toJSON(task_result, auto_unbox = T))
  })
  
  r <- as_completed(task_queue, fts, pop = T, n = 10, function(ft) {
    expect_true(ft$done())
    expect_equal(ft$eq_task_id, fromJSON(ft$result()[[2]])$j)
    ft$eq_task_id
  })
  
  completed_ids <- r$f_results
  fts <- r$fts
  expect_equal(10, length(completed_ids))
  expect_equal(40, length(fts))
  uncompleted_ids <- lapply(fts, function(x) x$eq_task_id)
  expect_false(any(completed_ids %in% uncompleted_ids))
  
  r <- as_completed(task_queue, fts, pop = T, n = 10, function(ft) {
    expect_true(ft$done())
    expect_equal(ft$eq_task_id, fromJSON(ft$result()[[2]])$j)
    ft$eq_task_id
  })
  
  completed_ids <- append(completed_ids, r$f_results)
  fts <- r$fts
  expect_equal(20, length(completed_ids))
  expect_equal(30, length(fts))
  uncompleted_ids <- lapply(fts, function(x) x$eq_task_id)
  expect_false(any(completed_ids %in% uncompleted_ids))
})


test_that("as_completed_timeout", {
  eqsql <- init_eqsql(python_path = venv, eqsql_path = eqpath)
  task_queue <- setup_task_queue(eqsql)
  
  # submit tasks
  fts <- lapply(c(1:50), function(x) {
    payload <- create_payload(x)
    sub_result <- task_queue$submit_task('eq_test', 0, payload)
    expect_equal(eqsql$task_queues$core$ResultStatus$SUCCESS, sub_result[[1]])
    sub_result[[2]]
  })
  
  expect_equal(50, length(fts))
  expect_error(as_completed(eqsql, fts, function(x){}, timeout = 2.0))
})

test_that("pop_completed", {
  eqsql <- init_eqsql(python_path = venv, eqsql_path = eqpath)
  task_queue <- setup_task_queue(eqsql)
  
  # submit tasks
  fts <- lapply(c(1:50), function(x) {
    payload <- create_payload(x)
    sub_result <- task_queue$submit_task('eq_test', 0, payload, priority = x)
    expect_equal(eqsql$task_queues$core$ResultStatus$SUCCESS, sub_result[[1]])
    sub_result[[2]]
  })
  
  expect_error(pop_completed(task_queue, fts, timeout = 1.0))
  
  # complete 1 task and push result
  result <- task_queue$query_task(0, timeout = 0.0)
  task_id <- result$eq_task_id
  task_result = list(j=task_id)
  task_queue$report_task(task_id, 0, toJSON(task_result, auto_unbox = T))
  
  result <- pop_completed(task_queue, fts)
  ft <- result[[2]]
  expect_false(is.null(ft))
  expect_equal(ft$eq_task_id, 50)
  new_fts <- result[[1]]
  expect_equal(length(new_fts), length(fts) - 1)
  uncompleted_ids <- lapply(new_fts, function(x) x$eq_task_id)
  expect_false(50 %in% uncompleted_ids)
})
