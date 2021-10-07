#!/bin/zsh -f
set -eu

# TEST PY 1 SH

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python

source $EQ_SQL/db/db-settings.sh

which python3
python3 $EQ_SQL/test/test-py-1-me.py &
JOB=${!}
python3 $EQ_SQL/test/test-py-1-wf.py
wait $JOB
