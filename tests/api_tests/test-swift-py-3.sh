#!/bin/zsh -f
set -eu

# TEST SWIFT PY 3 - 

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/../.. )
export EQ_SQL
source $THIS/db_env.sh
source $EQ_SQL/db/db-settings.sh

export PYTHONPATH=$EQ_SQL/python:$EQ_SQL/swift-t/ext

export EQ_DB_RETRY_THRESHOLD=0
export EQ_QUERY_TASK_TIMEOUT=120.0

PROCS=6
export TURBINE_RESIDENT_WORK_WORKERS=1
export RESIDENT_WORK_RANK=$(( PROCS - 2 ))



which python3
python3 $EQ_SQL/tests/api_tests/test-swift-3-me.py &
swift-t -n $PROCS -p -I $EQ_SQL/swift-t/ext $EQ_SQL/tests/api_tests/worker_pool_batch.swift &

wait
