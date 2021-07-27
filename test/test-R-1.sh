#!/bin/zsh -f
set -eu

# TEST R 1

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )

which Rscript
Rscript $EQ_SQL/test/test-R-1-me.R &
sleep 1
Rscript $EQ_SQL/test/test-R-1-wf.R

wait
