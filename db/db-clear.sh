#!/bin/bash
set -eu

# DB CLEAR SH
# Deletes all the tables!
# Set environment variable DB_CONFIRM=0 to skip confirmations

if (( ${#} != 0 ))
then
  echo "Too many arguments!"
  exit 1
fi

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh -v $*

DB_DELAY=5
if (( ${DB_CONFIRM:-1} ))
then
  echo
  echo "Deleting all tables ... Enter to confirm ... Ctrl-C to cancel ..."
  read _
  echo "Deleting ..."
  echo
  # ignore non-zero exit code for no input:
  read -t 5 _ || true 
else
  DB_DELAY=1
fi

sql <<EOF
\set ON_ERROR_STOP on
\dt
select pg_sleep($DB_DELAY);
drop table exp_runs;
drop table exp_instnces;
drop table expids;
EOF

echo "db-clear.sh: OK"
