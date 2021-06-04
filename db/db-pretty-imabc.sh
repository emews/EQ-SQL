#!/bin/bash
set -eu

# DB PRETTY IMABC SH
# Show human-readable info

THIS=$( readlink --canonicalize $( dirname $0 ) )
source $THIS/db-settings.sh || exit 1
export DB_MODE=ON

export ADLB_RANK_SELF=0

if (( ${#} > 0 )) && [[ $1 == "--" ]]
then
  shift
fi

set -x
which python
python $THIS/db-pretty-imabc.py $*
