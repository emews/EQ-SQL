#!/bin/bash
set -eu

# TEST SWIFT 4
# Simple R ME with JSON handling

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL

$THIS/check-queues.sh

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python:$THIS

swift-t -I $EQ_SQL/swift $EQ_SQL/swift/loopj-type.swift &
JOB=${!}
Rscript $THIS/test-swift-4-me.R

wait $JOB
