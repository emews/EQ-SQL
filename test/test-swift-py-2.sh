#!/bin/zsh -f
set -eu

# TEST SWIFT PY 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL
source $EQ_SQL/db/db-settings.sh

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python


which python3
python3 $EQ_SQL/test/test-swift-2-me.py &
swift-t -I $EQ_SQL/swift -I $EQ_SQL/pools/swift-t/ext/emews $EQ_SQL/swift/loop.swift &

wait
