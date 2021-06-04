#!/bin/bash

# DB CREATE SH
# Use createdb to create the DB
# The DB must be running for this to work (cf. db-start.sh)

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh -v $*

(
  set -x
  createdb --host=$DB_HOST --port=$DB_PORT CoW
)

echo "db-create.sh: OK"
