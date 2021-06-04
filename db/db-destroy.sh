#!/bin/bash
set -eu

# DB DESTROY SH

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh -dv $*

if [[ ! -d $DB_DATA ]]
then
  echo "db-destroy.sh: ERROR: Does not exist: DB_DATA=$DB_DATA"
  exit 1
fi

if db-ping.sh
then
  echo "db-destroy.sh: ERROR: The DB is running!"
  echo "db-destroy.sh:        Stop it with db-stop.sh"
  exit
fi

echo
echo "db-destroy: Good - the DB server is not running.  OK ..."
echo

DB_DELAY=${DB_DELAY:-5}
if (( ${DB_CONFIRM:-1} ))
then
  echo
  echo "Deleting the whole DB ... Enter to confirm ... Ctrl-C to cancel ..."
  read _
  echo "Deleting ..."
  echo
  read -t 5 _ || true
else
  DB_DELAY=1
fi

set -x
sleep $DB_DELAY
rm -r $DB_DATA
