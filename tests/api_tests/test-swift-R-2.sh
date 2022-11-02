#!/bin/zsh -f
set -eu

# TEST SWIFT R 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/../.. )
export EQ_SQL
source $EQ_SQL/db/db-settings.sh

export PYTHONPATH=$EQ_SQL/swift-t/ext:$EQ_SQL/python

which python3
Rscript $EQ_SQL/tests/api_tests/test-swift-2-me.R &
swift-t -I $EQ_SQL/swift-t/ext $EQ_SQL/tests/api_tests/worker_pool.swift &

wait
