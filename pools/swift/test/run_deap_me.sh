#!/bin/bash
set -eu

# TEST SWIFT 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/../../.. )
export EQ_SQL

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python:$EQ_SQL/test

export DB_HOST=beboplogin3.lcrc.anl.gov
source $EQ_SQL/db/db-settings.sh
python $EQ_SQL/test/test-swift-2-me.py
