#!/bin/zsh -f
set -eu

# TEST SWIFT PY 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/../.. )
export EQ_SQL
source $EQ_SQL/db/db-settings.sh

export PYTHONPATH=$EQ_SQL/python:$EQ_SQL/swift-t/ext

export EQ_DB_RETRY_THRESHOLD=0
export EQ_QUERY_TASK_TIMEOUT=120.0

which python3
python3 $EQ_SQL/tests/api_tests/test-swift-2-me.py &
swift-t -I $EQ_SQL/swift-t/ext $EQ_SQL/tests/api_tests/worker_pool.swift &

wait
