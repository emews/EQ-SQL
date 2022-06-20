library(DBI)

db_host <- Sys.getenv("DB_HOST")
db_user <- Sys.getenv("DB_USER")
db_port <- Sys.getenv("DB_PORT")
db_name <- Sys.getenv("DB_NAME")

conn <- NULL
retries <- 0
start  <- Sys.time()
while (is.null(conn)) {
    conn <- tryCatch({
        DBI::dbConnect(RPostgres::Postgres(),
                    dbname = db_name, host = db_host, port = db_port,
                    user = db_user)
       
    }, error = function(err) {
        delay <- runif(1) * 4
        retries <<- retries + 1
        Sys.sleep(delay)
    })
}

end <- Sys.time()
cat(paste0(retries, ',', end - start, "\n"))
dbAppendTable(conn, 'iris', iris)
dbDisconnect(conn)