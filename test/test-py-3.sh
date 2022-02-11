#!/bin/zsh -f
set -eu

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

source $EQ_SQL/db/db-settings.sh
$THIS/check-queues.sh

which python3
python3 $EQ_SQL/test/test-py-3-me.py &
python3 $EQ_SQL/test/test-py-3-wf.py

wait
