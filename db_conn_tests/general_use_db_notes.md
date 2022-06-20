## One Time Steps ##

Clone EQ/SQL to get the database utilities.

```bash
$ git clone git@github.com:emews/EQ-SQL.git
```

Database utilities are in `EQ-SQL/db`

Create the data directory. 

```bash
$ cd EQ-SQL/db
$ source env-bebop.sh
# Using '/lcrc/project/EMEWS/db/plima' as an example data directory
$ export DB_DATA=/lcrc/project/EMEWS/db/plima
$ initdb -D $DB_DATA -g
$ cp sample_postgresql.conf $DB_DATA/postgresql.conf
$ cp sample_pg_hba.conf $DB_DATA/pg_hba.conf
```

Start the server and create a database within the data directory

```bash
$ cd EQ-SQL/db
$ source env-bebop.sh
# Using '/lcrc/project/EMEWS/db/plima' as an example data directory
$ export DB_DATA=/lcrc/project/EMEWS/db/plima
$ export DB_NAME=EQ_SQL
# Start the db server
$ ./db-start.sh
$ createdb --host=$DB_HOST --port=$DB_PORT $DB_NAME
# Confirm the database was created
$ ./db-ping.sh
# Stop the db server
$ ./db-stop.sh
```

R Requirements

* If using R other than `/lcrc/project/EMEWS/bebop/repos/spack/opt/spack/linux-centos7-broadwell/gcc-7.1.0/r-4.0.0-plchfp7jukuhu5oity7ofscseg73tofx/bin/R`, install the R packages DBI, and RPostgres.

## Starting the Databse ##

Before any workflow that uses the DB is run, the server must be 
started. The scripts in the EQ-SQL/db are used to start and stop
the database.

```bash
$ export DB_DATA=/lcrc/project/EMEWS/db/plima
$ cd EQ-SQL/db
$ source env-bebop.sh
$ ./db-start.sh
```

When you start the database, the DB_HOST, DB_PORT and other environment variables
will be displayd as output. Copy and save these. You will need them when creating
a database connection in R or Python code. These values will also be saved to a
db_env_vars_N.txt file where N is timestamp.

## Stop the Database ##

Currently to stop the database, you *MUST* be logged onto the same login node where the database was
started. So, if in the `db-start.sh` output, you see:

`DB_HOST=beboplogin4.lcrc.anl.gov`

then you must login to `beboplogin4` to stop the database, and do the following:

```bash
$ cd EQ-SQL/db
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
# dbCreateTable(conn, ...)
# dbAppendTable(conn, ...)

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



