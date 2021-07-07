#!/bin/zsh -f
set -eu

# TEST 1

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

which python3
python3 $EQ_SQL/test/py-test-1-me.py &
python3 $EQ_SQL/test/py-test-1-wf.py

wait
