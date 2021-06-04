#!/bin/bash
set -eu

# DB SHOW EXP SH
# Show stats for this EXP

if (( ${#} == 1 ))
then
  EXPID=$1
else
  echo "Provide EXPID!"
  exit 1
fi

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh || exit 1

export ADLB_RANK_SELF=0

set -x
which python
python $THIS/db-stats-exp.py $EXPID
