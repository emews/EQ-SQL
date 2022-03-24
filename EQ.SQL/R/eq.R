library(logger)
library(glue)

#' @export
ResultStatus <- list(SUCCESS=0, FAILURE=1)

EQ_ABORT <- 'EQ_ABORT'
EQ_TIMEOUT <- 'EQ_TIMEOUT'
EQ_FINAL <- 'EQ_FINAL'


#' @export
eq.init <- function() {
    success <- TRUE  # assume success
    db_port = Sys.getenv("DB_PORT")
    if (nchar(db_port) == 0) {
        success <- FALSE
        logger::log_error("Set environment variable db_port!")
    }
    db_name = Sys.getenv("DB_NAME")
    if (nchar(db_name) == 0) {
        success <- FALSE
        logger::log_error("Set environment variable db_name!")
    }

    db_host = 'localhost'
    if (nchar(Sys.getenv("DB_HOST")) != 0) {
        db_host = Sys.getenv("DB_HOST")
    }

    db_user = Sys.getenv('USER')
    if (nchar(Sys.getenv("DB_USER")) != 0) {
        db_host = Sys.getenv("DB_USER")
    }

    if (! success) {
        return(FALSE)
    }

    # logger::log_info(paste0('EQ.SQL connecting: dbname = ', db_name, ', host = ', db_host, ', port = ',
    #                 db_port, ', user = ',db_user))
    logger::log_info('EQ.SQL connecting: dbname = {db_name}, host = {db_host}, port = {db_port},
                     {db_port}, user = {db_user}')
    conn <<- DBI::dbConnect(RPostgres::Postgres(),
                            dbname = db_name, host = db_host, port = db_port,
                            user = db_user)
    
    TRUE
}


#' @export
eq.submit.task <- function(exp_id, eq_type, payload, priority=0) {
    eq_task_id <- DB_submit(exp_id, eq_type, payload)
    eq.OUT_put(eq_type, eq_task_id, priority)
    eq_task_id
}

#' @export
# Queries the database for work of the specified type. 
# A named list formatted message. If the query results in a
# status update, the dictionary will have the following format:
# ('type': 'status', 'payload': P} where P is one of 'EQ_FINAL',
# 'EQ_ABORT', or 'EQ_TIMEOUT'. If the query specifies work to be done
# then the dictionary will be:  {'type': 'work', 'eq_task_id': eq_task_id, 
# 'payload': P} where P is the parameters for the work to be done.
eq.query.task <- function(eq_type, timeout=2.0) {
    status_rs <- eq.OUT_get(eq_type, timeout=timeout)
    status = status_rs[[1]]
    result = status_rs[[2]]
    if (status == ResultStatus$SUCCESS) {
        eq_task_id <- result
        # print(paste0("eq_task_id ", eq_task_id))
        payload = DB.json.out(eq_task_id)
        # print(paste0('Payload: ', payload))
        if (payload == EQ_FINAL) {
            return(list(type='status', payload='EQ_FINAL'))
        } else {
            return(list(type='work', eq_task_id=eq_task_id, payload=payload))
        }
    } else {
        return(list(type='status', payload=result))
    }
}

#' @export
eq.report.task <- function(eq_type, eq_task_id, result) {
    # Reports the result of the specified task of the specified type
    DB_result(eq_task_id, result)
    SQL.insert('emews_queue_IN', list("eq_task_type",  "eq_task_id"),
                list(eq_type, eq_task_id))
}


#' @export
eq.query.result <- function(eq_task_id, delay, timeout) {
    msg <- eq.IN_get(eq_task_id)
    if (msg[[1]] != ResultStatus$SUCCESS) {
        return(msg)
    }
    result <- DB.json.in(eq_task_id)    
    list(ResultStatus$SUCCESS, result)
}

# Python doc:
# """Queries for the result of the specified task.

#     The query repeated polls for a result. The polling interval is specified by
#     the delay such that the first interval is defined by the initial delay value
#     which is increased exponentionally after the first poll. The polling will
#     timeout after the amount of time specified by the timout value is has elapsed.

#     Args:
#         eq_task_id: the id of the task to query
#         delay: the initial polling delay value
#         timeout: the duration after which the query will timeout

#     Returns:
#         A tuple whose first element indicates the status of the query:
#         ResultStatus.SUCCESS or ResultStatus.FAILURE, and whose second element
#         is either result of the task, or in the case of failure the reason
#         for the failure (EQ_TIMEOUT, or EQ_ABORT)

#     """
#' @export
eq.done <- function(msg) {
    if (msg == EQ_FINAL) {
        return(TRUE)
    }

    if (msg == EQ_ABORT | msg == EQ_TIMEOUT) {
        logger::log_warn(sprintf("eq.done(): %s", msg))
        return (TRUE)
    }
    FALSE
}

  
#' @export
DB.json.out <- function(eq_task_id) {
    rs <- dbGetQuery(conn, "select json_out from eq_tasks where eq_task_id = $1", 
                     params=list(eq_task_id))
    # joq <- dbSendQuery(conn, "select json_out from eq_tasks where eq_task_id = ?")
    # dbBind(joq, list(eq_task_id))
    # rs <- dbFetch(joq)
    # Convert SQL integer64 to R integer:
    result = rs[1,1]
    datetime.fmt <- "%FT%T%z" 
    ts <- format(Sys.time(), datetime.fmt)
    SQL.update("eq_tasks", list('time_start'), list(Q(ts)), 
               where=glue::glue('eq_task_id={eq_task_id}'))
    result
}


#' @export
DB.json.in <- function(eq_task_id) {
    # returns the 'json_in' value for the specified eq_task_id
    rs <- dbGetQuery(conn, "select json_in from eq_tasks where eq_task_id = $1",
                     params=list(eq_task_id))
    result = rs[1,1]
    result
}


#' @export
eq.OUT_put <- function(eq_type, eq_task_id, priority=0) {
    SQL.insert('emews_queue_OUT', list("eq_task_type",  "eq_task_id", "eq_priority"),
               list(eq_type, eq_task_id, priority))
}

#' @export
eq.OUT_get <- function(eq_type, delay=0.5, timeout=2.0) {
    # returns a list with two elements - ResultStatus and the data
    # associated with that status -- i.e., EQ_ABORT or EQ_TIMEOUT 
    # on failure or the payload on success
    sql_pop <- sql_pop_out_q(eq_type)
    res <- queue_pop(sql_pop, delay, timeout)
    res
}

#' @export
eq.IN_get <- function(eq_task_id, delay=0.5, timeout=2.0) {
    # returns a list with two elements - ResultStatus and the data
    # associated with that status -- i.e., EQ_ABORT or EQ_TIMEOUT 
    # on failure or the payload on success
    sql_pop <- sql_pop_in_q(eq_task_id)
    res <- queue_pop(sql_pop, delay, timeout)
    res
}

#' @export
eq.DB.final <- function(eq_type) {
    eq_task_id <- SQL.ID()
    SQL.insert('eq_tasks', list("eq_task_id", "eq_task_type", "json_out"),
               list(eq_task_id , eq_type, Q(EQ_FINAL)))
    eq.OUT_put(eq_type, eq_task_id)
    eq_task_id
}

SQL.ID <- function () {
    rs <- dbGetQuery(conn, "select nextval('emews_id_generator');")
    # Convert SQL integer64 to R integer:
    id <- as.integer(rs[1,1])
    id
}

queue_push <- function(table, eq_id, eq_type) {
    SQL.insert(table, list("eq_id", "eq_type"),
                        list( eq_id,   eq_type))
}

DB_submit <- function(exp_id, eq_type, payload) {
    eq_task_id <- SQL.ID()
    datetime.fmt <- "%FT%T%z" 
    ts <- format(Sys.time(), datetime.fmt)
    SQL.insert("eq_tasks", list("eq_task_id", "eq_task_type", "json_out", "time_created"),
              list(eq_task_id , eq_type, Q(payload), Q(ts)))
    SQL.insert("eq_exp_id_tasks", list("exp_id", "eq_task_id"),
              list(Q(exp_id), eq_task_id))
    eq_task_id
}

DB_result <- function(eq_task_id, payload) {
    datetime.fmt <- "%FT%T%z" 
    ts <- format(Sys.time(), datetime.fmt)
    SQL.update('eq_tasks', list("json_in", 'time_stop'), list(Q(payload), Q(ts)),
               where=sprintf('eq_task_id=%i', eq_task_id))
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


queue_pop <- function(sql_pop, delay, timeout) {
    start  <- Sys.time()
    result <- tryCatch(
        {
            repeat {
                rs <- dbSendQuery(conn, sql_pop)
                df <- dbFetch(rs)
                dbClearResult(rs)
                if (nrow(df) > 0) {
                    return(list(ResultStatus$SUCCESS, df[1, 2]))
                }
                if (Sys.time() - start > timeout) {
                    return (list(ResultStatus$FAILURE, EQ_TIMEOUT))
                }
                delay <- delay * runif(1) * 2
                Sys.sleep(delay)
                delay <- delay * delay
            }
        }, error=function(cond) {
            # logger::log_error(cond)
            message(cond)
            return(list(ResultStatus$FAILURE, EQ_ABORT))
        }
    )
    result
}

#' @export
SQL.insert <- function(table, names, values) {
  # print("insert")
  n <- SQL.tuple(names)
  # cat("SQL.insert: values: ", paste(values), "\n")
  v <- SQL.tuple(values)
  # cat("SQL.insert: v: ", paste(v), "\n")
  cmd <- sprintf("insert into %s %s values %s;", table, n, v)
  rs <- SQL.execute(cmd)
  # cat("SQL.insert: result=", rs, "\n")
}

#' @export
SQL.update <- function(table, names, values, where) {
  # cat("update\n")
  if (length(names) != length(values)) {
    crash("update: lengths do not agree!")
  }
  # cat("update", length(names), "\n")
  assign_list <- list()
  for (i in 1:length(names)) {
    # cat(i, "\n")
    name  = names[[i]]
    value = values[[i]]
    assign_list <- append(assign_list, sprintf("%s=%s", name, value))
    # cat(i, unlist(assign_list), "\n") 
  }
  assigns = paste(assign_list, collapse=",")
  # cat("end", unlist(assigns), "\n")
  cmd <- sprintf("update %s set %s where %s;", table, assigns, where)
  # cat("update cmd: ", cmd, "\n")
  rs <- SQL.execute(cmd)
  # cat("SQL.update:   result=", rs, "\n")
}

SQL.tuple <- function(L) {
  # cat("SQL.tuple: ", paste(L, collapse=","), "\n")
  sprintf("(%s)", paste(L, collapse=","))
}

SQL.execute <- function(cmd) {
  # cat("SQL.execute: ", cmd, "\n")
  # Returns number of affected rows:
  dbExecute(conn, cmd)
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
  quit(status=1)
}

# Local Variables:
# ess-indent-level: 4
# eval: (flycheck-mode)
# End:
