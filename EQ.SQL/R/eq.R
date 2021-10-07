
#' @export
eq.init <- function() {
  # print("init")
  success <- TRUE  # assume success
  DB_PORT = Sys.getenv("DB_PORT")
  if (nchar(DB_PORT) == 0) {
    success <- FALSE
    print("Set environment variable DB_PORT!")
  }
  DB_NAME = Sys.getenv("DB_NAME")
  if (nchar(DB_NAME) == 0) {
    success <- FALSE
    print("Set environment variable DB_NAME!")
  }
  if (! success) {
    return(FALSE)
  }
  # cat(DB_PORT, "\n")
  # cat(DB_NAME, "\n")
  # print(DB_PORT, DB_NAME)

  conn <<- DBI::dbConnect(RPostgres::Postgres(),
                         dbname = DB_NAME, host = "localhost", port = DB_PORT)
  # print(conn)
  # print("OK")
  TRUE
}

#' @export
eq.OUT_put <- function(eq_type, msg) {
  printf("OUT_put(eq_type=%i, '%s')\n", eq_type, msg)
  queue_push("emews_queue_OUT", eq_type, Q(msg))
}

#' @export
eq.OUT_get <- function(eq_type, delay, timeout) {

  result <- queue_pop("emews_queue_OUT", eq_type, delay, timeout)

  if (result != FALSE) {
    result
  } else {
    print("eq.R:OUT_get(): popped nothing: abort!")
    "EQ_ABORT"
  }
}

#' @export
eq.IN_get <- function(eq_type) {
  result = queue_pop("emews_queue_IN", eq_type, 1, 5)
  if (result == FALSE) {
    print("eq.IN_get(): nothing to pop!")
    quit(status=1)
  } else {
    result
  }
}

SQL.ID <- function () {
  rs <- dbGetQuery(conn, "select nextval('emews_id_generator');")
  # Convert SQL integer64 to R integer:
  id <- as.integer(rs[1,1])
}

queue_push <- function(table, eq_type, value) {
  cat("\n")
  # rs <- dbGetQuery(conn, "select nextval('emews_id_generator');")
  # Convert SQL integer64 to R integer:
  # id <- as.integer(rs[1,1])
  id <- SQL.ID()
  SQL.insert(table,
             list("eq_id", "eq_type", "json"),
             list(    id,   eq_type ,  value))
}

sql_pop_q <- function(table, eq_type) {
  # Generate code for a queue pop from given table
  #   From: https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
  template <- "
    DELETE FROM %s
    WHERE eq_type = %i AND eq_id = (
    SELECT eq_id
    FROM %s
    ORDER BY eq_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    "
  sprintf(template, table, eq_type, table)
}

queue_pop <- function(table, eq_type, delay, timeout) {
  sql_pop <- sql_pop_q(table, eq_type)
  start  <- Sys.time()
  success <- FALSE
  repeat {
    if (Sys.time() - start > timeout) {
      break  # timeout
    }
    delay <- delay * runif(1) * 2
    Sys.sleep(delay)
    rs <- dbSendQuery(conn, sql_pop)
    df <- dbFetch(rs)
    msg <- df[1,3]
    count <- nrow(df)
    dbClearResult(rs)
    printf("%0.1f queue_pop(%s): count=%i\n", Sys.time(), table, count)
    if (count > 0) {
      success <- TRUE
      break
    }
    delay <- delay * 2
  }
  if (! success) {
    printf("queue_pop(%s): TIME OUT\n", table)
    FALSE
  } else {
    printf("queue_pop(%s): got: %s\n", table, msg)
    msg
  }
}

SQL.insert <- function(table, names, values) {
  # print("insert")
  n <- SQL.tuple(names)
  # cat("SQL.insert: values: ", paste(values), "\n")
  v <- SQL.tuple(values)
  # cat("SQL.insert: v: ", paste(v), "\n")
  cmd <- sprintf("insert into %s %s values %s;", table, n, v)
  rs <- SQL.execute(cmd)
  cat("SQL.insert: result=", rs, "\n")
}

SQL.tuple <- function(L) {
  # cat("SQL.tuple: ", paste(L, collapse=","), "\n")
  sprintf("(%s)", paste(L, collapse=","))
}

SQL.execute <- function(cmd) {
  cat("SQL.execute: ", cmd, "\n")
  # Returns number of affected rows:
  dbExecute(conn, cmd)
}

Q <- function(s) {
  # Just quote the given string
  paste0("'", s, "'")
}

# Local Variables:
# ess-indent-level: 2
# eval: (flycheck-mode)
# End:
