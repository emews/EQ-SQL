#!/bin/bash
set -eu

# DB EXPERIMENT INIT SH
# Start an experiment
# usage: db-exp-init.sh <DB_NAME>? <EXPID>

if (( ${#} != 1 ))
then
  echo "db-exp-init.sh: ERROR: Provide <EXPID>"
  exit 1
fi

EXPID=$1

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh

if [[ $DB_MODE == "OFF" ]]
then
  exit
fi

echo "db-exp-init.sh: using DB_HOST=$DB_HOST DB_PORT=$DB_PORT ..."
echo

echo -n "db-exp-init.sh: inserting EXPID='$EXPID' as exp_int: "
if sql -q -t <<EOF
\set ON_ERROR_STOP on
INSERT INTO expids (expid) VALUES ('$EXPID');
SELECT exp_int FROM expids WHERE expid='$EXPID';
EOF
then
  : OK
else
  echo "$0: SQL failed during experiment init!"
  if [[ $DB_MODE == "SOFT" ]]
  then
    exit 0 # proceed even if DB is missing
  else
    echo "$0: $DB_MODE==SOFT : abort."
    exit 1
  fi
fi

echo "db-exp-init.sh: OK"
