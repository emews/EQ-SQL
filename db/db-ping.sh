#!/bin/bash
set -eu

# DB PING
# Provide -v for verbose output

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh -v $*

if sql < /dev/null
then
  echo "db-ping.sh: OK"
else
  echo "db-ping.sh: COULD NOT CONNECT!"
  exit 1
fi
