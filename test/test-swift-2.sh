#!/bin/zsh -f
set -eu

# TEST SWIFT 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

which python3
python3 $EQ_SQL/test/test-swift-2-me.py &
swift-t -I $EQ_SQL/swift $EQ_SQL/swift/loop.swift &

wait
