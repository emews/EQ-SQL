## One Time Steps ##

Clone EQ/SQL to get the database utilities.

```bash
$ git clone git@github.com:emews/EQ-SQL.git
```

* If using R other than `/lcrc/project/EMEWS/bebop/repos/spack/opt/spack/linux-centos7-broadwell/gcc-7.1.0/r-4.0.0-plchfp7jukuhu5oity7ofscseg73tofx/bin/R`, then install the R packages DBI, and RPostgres.

## Starting the Databse ##

The scripts in the EQ-SQL/db are used to start and stop
the database.

```bash
$ DB_DATA=/lcrc/project/EMEWS/db/plima
$ cd EQ-SQL/db
$ source env-bebop.sh
$ ./db-start.sh
```

When you start the database, copy and save the DB_HOST, and DB_PORT values in the output. You will
need these values to stop the database later, if you close the terminal where you started
the database.

```bash
$ ./db-start.sh 

# Output:
DB SETTINGS:
DB_HOST=beboplogin1.lcrc.anl.gov
DB_PORT=11219
DB_NAME=EQ_SQL
...
```

## Stop the Database ##

```bash
$ cd EQ-SQL/db
$ export DB_HOST=<saved hostname>
$ export DB_PORT=<saved port>
$ export DB_DATA=/lcrc/project/EMEWS/db/plima
$ source env-bebop.sh
$ ./db-stop.sh
```

Note: If you haven't closed the terminal in which you started the db, then you can just do
`./db-stop.sh`

## Using the Database

Once the database is started, you can use the following code to connect to it and
append data tables / data frames to a database table. To get a connection:

```r
# Set these in your submission script to the values
# output when you started the database. For example,
# export DB_HOST=beboplogin1.lcrc.anl.gov
# export DB_USER etc.
db_host <- Sys.getenv("DB_HOST")
db_user <- Sys.getenv("DB_USER")
db_port <- Sys.getenv("DB_PORT")
db_name <- Sys.getenv("DB_NAME")


# Repeatedly attempt to get a DB connection with some delay 
# between re-tries. There is a limited number of connections
# and these can be easily exhaused by many swift workers 
# attempting to connect at once.
conn <- NULL
while (is.null(conn)) {
    conn <- tryCatch({
        DBI::dbConnect(RPostgres::Postgres(),
                    dbname = db_name, host = db_host, port = db_port,
                    user = db_user)
       
    }, error = function(err) {
        delay <- runif(1) * 4
        Sys.sleep(delay)
    })
}

# Use the connection
# dbCreateTable(...)
# dbAppendTable(...)

# Close the conn each time after it's used
# There's a max number of concurrent connections and so
# it needs to be released.
dbDisconnect(conn)
```

You can create the database table for your R data table with `dbCreateTable(conn, table_name, datatable)`
and append to the table with  `dbAppendTable(conn, table_name, datatable)`. You only need to
create the table once, and can append to it after that. Each of these takes a db connection object,
instantiated by the code above, which should be closed after dumping the datatable with 
`dbDisconnect(conn)`.



