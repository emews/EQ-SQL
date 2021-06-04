#!/bin/bash
set -eu

# DB TUNNEL SH
# Set up an SSH tunnel to the given host
#     using DB_PORT on both ends
#     and DB_USER as login name if set

if [[ ${#} != 1 ]]
then
  echo "Provide remote host name!"
  exit 1
fi

REMOTE_HOST=$1

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh $*

if (( ${#DB_USER} ))
then
  REMOTE_USER_ARG="-l $DB_USER"
fi

echo "db-tunnel: connecting to: $REMOTE_HOST $DB_PORT" \
     "as ${DB_USER:-${USER}} ..."

set -x
ssh ${REMOTE_USER_ARG:-} \
    -L $DB_PORT:localhost:$DB_PORT \
    $REMOTE_HOST
