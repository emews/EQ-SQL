#!/bin/bash
set -eu

# TEST SWIFT 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python:$EQ_SQL/test

source $EQ_SQL/db/db-settings.sh
$EQ_SQL/test/check-queues.sh

swift-t -I $EQ_SQL/swift $EQ_SQL/swift/loopj-type.swift &
JOB=${!}
python $THIS/test-swift-2-me.py

wait $JOB
