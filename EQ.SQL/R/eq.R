
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
eq.OUT_put <- function(msg) {
  printf("OUT_put(%s)\n", msg)
  # print(conn)
  queue_push("emews_queue_OUT", Q(msg))
}

#' @export
eq.OUT_get <- function(delay, timeout) {

  result <- queue_pop("emews_queue_OUT", delay, timeout)

  if (result != FALSE) {
    result
  } else {
    print("eq.R:OUT_get(): popped nothing: abort!")
    "EQ_ABORT"
  }
}

queue_push <- function(table, value) {
  cat("\n")
  rs <- dbGetQuery(conn, "select nextval('emews_id_generator');")
  cat("rs: _", rs[1,1], "_\n")
  # Convert SQL integer64 to R integer:
  id <- as.integer(rs[1,1])
  # print(id)
  # cat("cat id ", id, "\n")
  # cat("push: value", value, "\n")
  # printf("rs: %f\n", x)
  SQL.insert(table, list("eq_id", "json"), list(id, value))
}


sql_pop_q <- function(table) {
  # Generate code for a queue pop from given table
  #   From: https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5
  template <- "
    DELETE FROM %s
    WHERE eq_id = (
    SELECT eq_id
    FROM %s
    ORDER BY eq_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
    )
    RETURNING *;
    "
  sprintf(template, table, table)
}

queue_pop <- function(table, delay, timeout) {
  sql_pop <- sql_pop_q(table)
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
    msg <- df[1,2]
    count <- nrow(df)
    dbClearResult(rs)
    printf("%0.1f queue_pop(%s): '%i'\n", Sys.time(), table, count)
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
    msg
  }
}

SQL.insert <- function(table, names, values) {
  print("insert")
  n <- SQL.tuple(names)
  cat("SQL.insert: values: ", paste(values), "\n")
  v <- SQL.tuple(values)
  cat("SQL.insert: v: ", paste(v), "\n")
  cmd <- sprintf("insert into %s %s values %s;", table, n, v)
  rs <- SQL.execute(cmd)
  cat("result ", rs, "\n")
}

SQL.tuple <- function(L) {
  cat("SQL.tuple: ", paste(L, collapse=","), "\n")
  sprintf("(%s)", paste(L, collapse=","))
}

SQL.execute <- function(cmd) {
  cat("SQL.execute: ", cmd, "\n")
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
