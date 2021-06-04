#!/bin/sh

# DB CPLO INIT
# Shell UI wrapper for db-cplo-init.py

export THIS=$( readlink --canonicalize-existing $( dirname $0 ) )
export EMEWS_PROJECT_ROOT=$(   readlink --canonicalize-existing $THIS/.. )

export PYTHONPATH=$PWD
if python3 $THIS/db-create.py $*
then
  :
else
  code=$?
  echo "db-create.py failed! code=$code"
  exit $code
fi
