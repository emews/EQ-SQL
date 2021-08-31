#!/bin/bash
set -eu

# DB CONNECT
# Starts an interactive session using the EQ-SQL settings
# Provide -v for verbose output

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh $*

sql
