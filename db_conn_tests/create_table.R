library(DBI)

db_host <- Sys.getenv("DB_HOST")
db_user <- Sys.getenv("DB_USER")
db_port <- Sys.getenv("DB_PORT")
db_name <- Sys.getenv("DB_NAME")

conn <- DBI::dbConnect(RPostgres::Postgres(),
               dbname = db_name, host = db_host, port = db_port,
               user = db_user)

dbCreateTable(conn, 'iris', iris)
dbDisconnect(conn)