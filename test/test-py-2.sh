#!/bin/zsh -f
set -eu

# TEST PY 2

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

source $EQ_SQL/db/db-settings.sh

$EQ_SQL/test/check-queues.py

which python3
python3 $EQ_SQL/test/test-py-2-me.py &
python3 $EQ_SQL/test/test-py-2-wf.py

wait
