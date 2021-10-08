#!/bin/zsh -f
set -eu

# TEST R 1

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

source $EQ_SQL/db/db-settings.sh
$EQ_SQL/test/check-queues.sh

which Rscript
Rscript $EQ_SQL/test/test-R-1-me.R &
JOB=${!}
sleep 1
Rscript $EQ_SQL/test/test-R-1-wf.R

wait $JOB
