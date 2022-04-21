library(logger)
library(glue)

#' Two element named list to indicate the status of the result task queries
#'
#' ResultStatus$SUCCESS indicates a successful query and ResultStatus$FAILURE
#' indicates a failed query.
#' @export
ResultStatus <- list(SUCCESS = 0, FAILURE = 1)

#' @export
EQ_ABORT <- "EQ_ABORT"
#' @export
EQ_TIMEOUT <- "EQ_TIMEOUT"
#' @export
EQ_STOP <- "EQ_STOP"


#' Initializes the database connection
#'
#' This must be called before using any of the functions in this package
#'
#' @param db_port The port of the database to connect to. Defaults to the
#'   environment variable "DB_PORT".
#' @param db_name The name of the database to connect to. Defaults to the
#'   environment variable "DB_NAME".
#' @param db_host The host of the database to connect to. Defaults to the
#'   "localhost". If the "DB_HOST" environment variable is set then that
#'   will be used.
#' @param db_user The user name to connect to the database with. Defaults to
#'  the current user name. If the "DB_USER" environment variable is set
#'  then that will be used.
#' @param log_level the logging threshold level. Defaults to logger::WARN.
#' @return TRUE on a successful initialization otherwise FALSE.
#' @export
eq_init <- function(db_port=Sys.getenv("DB_PORT"),
                    db_name=Sys.getenv("DB_NAME"),
                    db_host="localhost",
                    db_user=Sys.getenv("USER"),
                    log_level=logger::WARN) {
    logger::log_threshold(log_level)
    success <- TRUE  # assume success
    if (nchar(db_port) == 0) {
        success <- FALSE
        logger::log_error("Set environment variable db_port!")
    }
    if (nchar(db_name) == 0) {
        success <- FALSE
        logger::log_error("Set environment variable db_name!")
    }

    if (nchar(Sys.getenv("DB_HOST")) != 0) {
        db_host <- Sys.getenv("DB_HOST")
    }

    if (nchar(Sys.getenv("DB_USER")) != 0) {
        db_user <- Sys.getenv("DB_USER")
    }

    if (! success) {
        return(FALSE)
    }

    logger::log_info("EQ.SQL connecting: dbname = {db_name}, host = {db_host},
                      port = {db_port},
                     {db_port}, user = {db_user}")
    conn <<- DBI::dbConnect(RPostgres::Postgres(),
                            dbname = db_name, host = db_host, port = db_port,
                            user = db_user)
    TRUE
}

#' Submits work of the specified type and priority with the specified
#' payload, returning the task id assigned to that task
#'
#' @param exp_id String. The id of the experiment of which the work is
#'      part.
#' @param eq_type integer. The type of work
#' @param payload String. The work payload
#' @param priority integer. The priority of this work
#' @return A named list with two elements "status", and "eq_task_id". The
#' first of these will be the ResultStatus. If the submission is successful
#' the "eq_task_id" will be the task id assigned to this task, otherwise the
#' "eq_task_id" will be -1.
#' @export
eq_submit_task <- function(exp_id, eq_type, payload, priority=0) {
    result <- insert_task(exp_id, eq_type, payload)
    if (result[[1]] == ResultStatus$SUCCESS) {
        eq_task_id <- result[[2]]
        status <- eq_push_out_queue(eq_task_id, eq_type, priority)
        if (status == ResultStatus$SUCCESS) {
            return(list(status = status, eq_task_id = eq_task_id))
        }
    }
    list(status = ResultStatus$FAILURE, eq_task_id = -1)
}

#' Queries for the highest priority task of the specified type
#'
#' The query repeatedly polls for a task. The polling interval is specified by
#' the delay such that the first interval is defined by the initial delay value
#' which is increased exponentionally after the first poll. The polling will
#' timeout after the amount of time specified by the timout value is has
#' elapsed.
#'
#' @param eq_type Integer. The type of the task to query for.
#' @param delay Numeric. The initial polling delay value.
#' @param timeout Numeric. The duration after which the query will timeout.
#'
#' @return A named list formatted message. If the query results in a
#'   status update, the list will have the following format:
#'   list(type="status", payload=P) where P is one of "EQ_STOP",
#'   "EQ_ABORT", or "EQ_TIMEOUT". If the query finds work to be done
#'   then the list will be: list(type="work", eq_task_id=eq_task_id,
#'   payload=P) where P is the parameters for the work to be done.
#' @export
eq_query_task <- function(eq_type, delay = 0.5, timeout=2.0) {
    status_rs <- eq_pop_out_queue(eq_type, delay = delay, timeout = timeout)
    status <- status_rs[[1]]
    result <- status_rs[[2]]
    if (status == ResultStatus$SUCCESS) {
        eq_task_id <- result
        payload_result <- select_task_payload(eq_task_id)
        if (payload_result[[1]] == ResultStatus$SUCCESS) {
            payload <- payload_result[[2]]
            if (payload == EQ_STOP) {
                return(list(type = "status", payload = "EQ_STOP"))
            } else {
                return(list(type = "work", eq_task_id = eq_task_id,
                    payload = payload))
            }
        } else {
            return(list(type = "status", payload = payload_result[[2]]))
        }
    }
    list(type = "status", payload = result)
}

#' Reports the result of the specified task of the specified type
#'
#' @param eq_task_id Integer. The id of the task whose results are being
#'  reported.
#' @param eq_type Integer. The type of the task whose results are being
#'  reported.
#' @param result String. The result of the task.
#' @return ResultStatus$SUCCESS if the task was successfully reported, otherwise
#'   ResultStatus$FAILURE.
#' @export
eq_report_task <- function(eq_task_id, eq_type, result) {
    # Reports the result of the specified task of the specified type
    result_status <- update_task(eq_task_id, result)
    if (result_status == ResultStatus$SUCCESS) {
        return(eq_push_in_queue(eq_task_id, eq_type))
    }
    result_status
}

#' Queries for the result of the specified task
#'
#' The query repeatedly polls for a result. The polling interval is specified by
#' the delay such that the first interval is defined by the initial delay value
#' which is increased exponentionally after the first poll. The polling will
#' timeout after the amount of time specified by the timout value is has
#' elapsed.
#'
#' @param eq_task_id Integer. The id of the task to query
#' @param delay Numeric. The initial polling delay value
#' @param timeout Numeric.The duration after which the query will timeout
#'
#' @return A two element list whose first element indicates the status of
#'   the query: ResultStatus$SUCCESS or ResultStatus$FAILURE, and whose
#'   second element is either the result of the task, or in the case of
#'   failure the reason for the failure ("EQ_TIMEOUT", or "EQ_ABORT")
#' @export
eq_query_result <- function(eq_task_id, delay, timeout) {
    msg <- eq_pop_in_queue(eq_task_id, delay, timeout)
    if (msg[[1]] != ResultStatus$SUCCESS) {
        return(msg)
    }
    select_task_result(eq_task_id)
}

#' Selects the "json_out" payload associated with the specified task id
#'
#' Selects the "json_out" payload for the specified task id from
#' the eq_tasks table, and also setting the start time of the task to the
#' current time
#'
#' @param eq_task_id Integer. The id of the task to get the json_out for
#' @return A list containing the ResultStatus as its first element,
#'   and if successful the json_out payload
#'   for the specified task id as its second, otherwise the second element will
#'   be EQ_ABORT.
#' @export
select_task_payload <- function(eq_task_id) {
    tcr <- tryCatch({
        # NOTE: "$1" syntax is specific to postgres
        rs <- dbGetQuery(conn,
                        "select json_out from eq_tasks where eq_task_id = $1",
                        params = list(eq_task_id))
        # Convert SQL integer64 to R integer:
        result <- rs[1, 1]
        datetime_fmt <- "%FT%T%z"
        ts <- format(Sys.time(), datetime_fmt)
        SQL_update("eq_tasks", list("time_start"), list(Q(ts)),
                where = glue::glue("eq_task_id={eq_task_id}"))
        list(ResultStatus$SUCCESS, result)
    }, error = function(cond) {
            logger::log_error("select_task_payload error: {conditionMessage(cond)}")
            return(list(ResultStatus$FAILURE, EQ_ABORT))
        }
    )
    tcr
}


#' Selects the result ("json_in") payload associated with the specified
#' task id in the eq_tasks table.
#'
#' @param eq_task_id Integer. The id of the task to get the json_in for.
#' @return A list containing the ResultStatus as its first element,
#'   and if successful the json_in result payload
#'   for the specified task id as its second, otherwise the second element will
#'   be EQ_ABORT.
#' @export
select_task_result <- function(eq_task_id) {
    tcr <- tryCatch({
        # returns the "json_in" value for the specified eq_task_id
        rs <- dbGetQuery(conn,
                "select json_in from eq_tasks where eq_task_id = $1",
                params = list(eq_task_id))
        result <- rs[1, 1]
        list(ResultStatus$SUCCESS, result)
    }, error = function(cond) {
        logger::log_error("select_task_result error: {conditionMessage(cond)}")
        return(list(ResultStatus$FAILURE, EQ_ABORT))
    })
    tcr
}

#' Pushes the specified task onto the output queue with
#' the specified priority.

#' @param eq_task_id Integer. The id of the task
#' @param eq_type Integer the type of the task
#' @param priority Integer the priority of the task
#' @return ResultStatus$SUCCESS if the task was successfully pushed
#'   onto the output queue, otherwise ResultStatus$FAILURE.
#' @export
eq_push_out_queue <- function(eq_task_id, eq_type, priority = 0) {
    tcr <- tryCatch({
        SQL_insert("emews_queue_OUT",
                list("eq_task_type",  "eq_task_id", "eq_priority"),
                list(eq_type, eq_task_id, priority))
        ResultStatus$SUCCESS
    }, error = function(cond) {
        logger::log_error("eq_push_out_queue error: {conditionMessage(cond)}")
        return(ResultStatus$FAILURE)
    })
    tcr
}

#' Pushes the specified task onto the input queue.

#' @param eq_task_id Integer. The id of the task
#' @param eq_type Integer the type of the task
#' @return ResultStatus$SUCCESS if the task was successfully pushed
#'   onto the input queue, otherwise ResultStatus$FAILURE.
#' @export
eq_push_in_queue <- function(eq_task_id, eq_type) {
    tcr <- tryCatch({
        SQL_insert("emews_queue_IN", list("eq_task_type",  "eq_task_id"),
                        list(eq_type, eq_task_id))
        ResultStatus$SUCCESS
    }, error = function(cond) {
        logger::log_error("eq_push_in_queue error: {conditionMessage(cond)}")
        return(ResultStatus$FAILURE)
    })
    tcr
}


#' Pops the highest priority task of the specified work type off
#' of the db out queue
#'
#' This call repeatedly polls for a task of the specified type. The polling
#' interval is specified by
#' the delay such that the first interval is defined by the initial delay value
#' which is increased exponentionally after the first poll. The polling will
#' timeout after the amount of time specified by the timout value is has
#' elapsed.
#'
#' @param eq_type. Integer. The type of the work to pop from the queue
#' @param delay. Numeric. The initial polling delay value
#' @param timeout. Numeric. The duration after which this call will timeout
#'   and return with an EQ_TIMEOUT result.
#'
#' @return A two element list whose first element is one of
#'   ResultStatus$SUCCESS or ResultStatus$FAILURE. On success the
#'   second element will be the popped eq task id. On failure, the second
#'   element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
#'   cause of the failure.
#' @export
eq_pop_out_queue <- function(eq_type, delay=0.5, timeout=2.0) {
    # returns a list with two elements - ResultStatus and the data
    # associated with that status -- i.e., EQ_ABORT or EQ_TIMEOUT
    # on failure or the payload on success
    sql_pop <- sql_pop_out_q(eq_type)
    res <- queue_pop(sql_pop, delay, timeout)
    res
}

#' Pops the specified task off of the db in queue
#'
#' This call repeatedly polls for a task with specified id. The polling
#' interval is specified by
#' the delay such that the first interval is defined by the initial delay value
#' which is increased exponentionally after the first poll. The polling will
#' timeout after the amount of time specified by the timout value is has
#' elapsed.
#'
#' @param eq_task_id Integer. The id of the task to pop
#' @param delay Numeric. The initial polling delay value
#' @param timeout Numeric. The duration after which this call will timeout
#'     and return.
#'
#' @return A two element list whose first element is one of
#'   ResultStatus$SUCCESS or ResultStatus$FAILURE. On success the
#'   second element will be the popped eq task id. On failure, the second
#'   element will be one of EQ_ABORT or EQ_TIMEOUT depending on the
#'   cause of the failure.
#' @export
eq_pop_in_queue <- function(eq_task_id, delay=0.5, timeout=2.0) {
    # returns a list with two elements - ResultStatus and the data
    # associated with that status -- i.e., EQ_ABORT or EQ_TIMEOUT
    # on failure or the eq_task_id on success
    sql_pop <- sql_pop_in_q(eq_task_id)
    res <- queue_pop(sql_pop, delay, timeout)
    res
}

#' Stops any workers pools associated with the specified work type by
#' pusing EQ_STOP into the output queue.
#'
#' @param eq_type Integer. The work type for the pools to stop
#' @return ResultStatus$SUCCESS if the stop message was successfully pushed,
#'   otherwise ResultStatus$FAILURE.
#' @export
eq_stop_worker_pool <- function(eq_type) {
    tcr <- tryCatch({
        eq_task_id <- next_task_id()
        SQL_insert("eq_tasks", list("eq_task_id", "eq_task_type", "json_out"),
                list(eq_task_id, eq_type, Q(EQ_STOP)))
        # this returns a ResultStatus
        eq_push_out_queue(eq_task_id, eq_type)
    }, error = function(cond) {
        logger::log_error("eq_stop_worker_pool error: {conditionMessage(cond)}")
        return(ResultStatus$FAILURE)
    })
    tcr
}

#' Gets the next eq_task_id from the database.
next_task_id <- function() {
    rs <- dbGetQuery(conn, "select nextval('emews_id_generator');")
    # Convert SQL integer64 to R integer:
    id <- as.integer(rs[1, 1])
    id
}

#' Inserts the specified payload to the database, creating
#' a task entry for it and returning its assigned task id
#'
#' @param exp_id Integer. The id of the experiment that this task is part of
#' @param eq_type Integer. The work type of this task
#' @param payload String. The task payload
#'
#' @return A list whose first element is the ResultStatus of the insert,
#'  and whose second element is the task id assigned to this task if the
#'  insert was successfull, otherwise EQ_ABORT.
#' @export
insert_task <- function(exp_id, eq_type, payload) {
    tcr <- tryCatch({
        eq_task_id <- next_task_id()
        datetime_fmt <- "%FT%T%z" 
        ts <- format(Sys.time(), datetime_fmt)
        SQL_insert("eq_tasks",
                list("eq_task_id", "eq_task_type", "json_out", "time_created"),
                list(eq_task_id, eq_type, Q(payload), Q(ts)))
        SQL_insert("eq_exp_id_tasks", list("exp_id", "eq_task_id"),
                list(Q(exp_id), eq_task_id))
        list(ResultStatus$SUCCESS, eq_task_id)
    }, error = function(cond) {
        logger::log_error("eq_stop_worker_pool error: {conditionMessage(cond)}")
        return(list(ResultStatus$FAILURE, EQ_ABORT))
    })
    tcr
}


#' Updates the specified task in the eq_tasks table with the specified
#' result ("json_in") payload
#'
#' This also updates the "time_stop" to the time when the update occurred.
#'
#' @param eq_task_id Integer. The id of the task to update
#' @param payload String. The payload to update the task with
#' @return ResultStatus$SUCCESS if the task was successfully updated,
#'   otherwise ResultStatus$FAILURE.
#' @export
update_task <- function(eq_task_id, payload) {
    tcr <- tryCatch({
        datetime_fmt <- "%FT%T%z" 
        ts <- format(Sys.time(), datetime_fmt)
        SQL_update("eq_tasks", 
                list("json_in", "time_stop"),
                list(Q(payload), Q(ts)),
                where = sprintf("eq_task_id=%i", eq_task_id))
        ResultStatus$SUCCESS
    }, error = function(cond) {
        logger::log_error("update_task error: {conditionMessage(cond)}")
        return(ResultStatus$FAILURE)
    })
    tcr
}

sql_pop_out_q <- function(eq_type) {
    # Generate code for a queue pop from emews_queue_out
    # From:
    # https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    # Can only select 1 column from the subquery,
    # but we return * from the deleted row.
    # See workflow.sql for the returned queue row
    template <- "
    DELETE FROM emews_queue_OUT
    WHERE  eq_task_id = (
    SELECT eq_task_id
    FROM emews_queue_OUT
    WHERE eq_task_type = %s
    ORDER BY eq_priority DESC, eq_task_id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    "
    sprintf(template, eq_type)
}

sql_pop_in_q <- function(eq_task_id) {
    # Generate code for a queue pop from emewws_queue_in
    # From:
    # https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
    # Can only select 1 column from the subquery,
    # but we return * from the deleted row.
    # See workflow.sql for the returned queue row
    template <- "
    DELETE FROM emews_queue_IN
    WHERE  eq_task_id = (
    SELECT eq_task_id
    FROM emews_queue_IN
    WHERE eq_task_id = %s
    ORDER BY eq_task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    "
    sprintf(template, eq_task_id)
}

#' Performs the actual queue pop as defined the sql string

#' This call repeatedly attempts the pop operation by executing sql until
#' the operation completes or the timeout duration has passed. The polling
#' interval is specified by the delay such that the first interval is
#' defined by the initial delay value which is increased exponentionally
#' after the first poll. The polling will timeout after the amount of time
#' specified by the timout value is has elapsed.
#'
#' @param sql_pop String. The sql query that defines the pop operation
#' @param delay Numeric. The initial polling delay value
#' @param timeout Numeric. The duration after which this call will timeout
#'   and return.
#'
#' @return A two element list where the first elements is one of
#'   ResultStatus$SUCCESS or ResultStatus$FAILURE. On success the
#'   second element will be the popped eq_task_id. On failure, the second
#'   element will be one of "EQ_ABORT" or "EQ_TIMEOUT" depending on the
#'   cause of the failure.
queue_pop <- function(sql_pop, delay, timeout) {
    start  <- Sys.time()
    tcr <- tryCatch({
        repeat {
            rs <- dbSendQuery(conn, sql_pop)
            df <- dbFetch(rs)
            dbClearResult(rs)
            if (nrow(df) > 0) {
                return(list(ResultStatus$SUCCESS, df[1, 2]))
            }
            if (Sys.time() - start > timeout) {
                return(list(ResultStatus$FAILURE, EQ_TIMEOUT))
            }
            delay <- delay * runif(1) * 2
            Sys.sleep(delay)
            delay <- delay * delay
        }
    }, error = function(cond) {
        logger::log_error("queue_pop error: {conditionMessage(cond)}")
        return(list(ResultStatus$FAILURE, EQ_ABORT))
    })
    tcr
}

#' Utility function for inserting the specified values into the specified
#' columns in the specified table.
#'
#' @param table String. The name of the table to insert to
#' @param col_names List. A list of column names to insert to
#' @param values List. A list of values to insert into the columns
#'
#' @return the result of the insert as a DBI result.
#' @export
SQL_insert <- function(table, col_names, values) {
  logger::log_debug("insert")
  n <- SQL_tuple(col_names)
  logger::log_debug("SQL_insert: values: {toString(values)}")
  v <- SQL_tuple(values)
  logger::log_debug("SQL_insert: v: {toString(v)}")
  cmd <- sprintf("insert into %s %s values %s;", table, n, v)
  rs <- dbExecute(conn, cmd)
  logger::log_debug("SQL_insert: result={rs}")
}

#' Utility function for updating the specified columns in the
#' specified table with the specified values.
#'
#' @param table String. The name of the table to update
#' @param col_names List. A list of column names to update
#' @param values List. A list of values to update the columns with
#' @param where String. A SQL where clause, NOT including the "where"
#' @return The number of rows effected by the update
#' @export
SQL_update <- function(table, col_names, values, where) {
  logger::log_debug("update")
  if (length(col_names) != length(values)) {
    crash("update: lengths do not agree!")
  }
  logger::log_debug("update {length(names)}")
  assign_list <- list()
  for (i in 1:length(col_names)) {
    # cat(i, "\n")
    name <- col_names[[i]]
    value <- values[[i]]
    assign_list <- append(assign_list, sprintf("%s=%s", name, value))
    # cat(i, unlist(assign_list), "\n") 
  }
  assigns <- paste(assign_list, collapse = ",")
  logger::log_debug("end {toString(assigns)}") # unlist(assigns), "\n")
  cmd <- sprintf("update %s set %s where %s;", table, assigns, where)
  logger::log_debug("update cmd: {cmd}")
  rs <- dbExecute(conn, cmd)
  logger::log_debug("SQL_update:   result={rs}")
}

SQL_tuple <- function(L) {
  # cat("SQL_tuple: ", paste(L, collapse=","), "\n")
  sprintf("(%s)", paste(L, collapse = ","))
}

#' @export
Q <- function(s) {
  # Just quote the given string
  paste0("'", s, "'")
}

#' @export
crash <- function(message) {
  cat(message)
  cat("\n")
  quit(status = 1)
}

# Local Variables:
# ess-indent-level: 4
# eval: (flycheck-mode)
# End:
