#!/bin/bash
set -eu

# TEST SWIFT 3

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL

$THIS/check-queues.sh

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

swift-t -I $EQ_SQL/swift $EQ_SQL/swift/loop.swift &
JOB=${!}
Rscript $THIS/test-swift-3-me.R

wait $JOB
