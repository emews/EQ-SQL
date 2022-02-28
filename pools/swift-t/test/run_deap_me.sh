#!/bin/bash
set -eu

# TEST SWIFT 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/../../.. )
export EQ_SQL
export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python:$EQ_SQL/test

export DB_HOST=beboplogin4.lcrc.anl.gov
# This is necessary if psycopg2 complains about missing symbols
export LD_LIBRARY_PATH=/lcrc/project/EMEWS/bebop/sfw/gcc-7.1.0/postgres-14.2/lib:$LD_LIBRARY_PATH

source $EQ_SQL/db/db-settings.sh
python $EQ_SQL/test/test-swift-2-me.py

