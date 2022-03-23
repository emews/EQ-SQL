#!/bin/bash
set -eu

# POOLS 1

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL

export PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python:$EQ_SQL/test

source $EQ_SQL/db/db-settings.sh

Rscript $THIS/test-pools-1-me.R &
JOB=${!}

TYPES=3
ITERATIONS=3
for (( i=1 ; i <= ITERATIONS ; i++ ))
do

  for (( type=1 ; type <= TYPES ; type++ ))
  do
    echo
    echo "Workflow eq_type=$type ..."
    swift-t -I $EQ_SQL/swift $EQ_SQL/swift/loopj-type.swift --eq-type=$type
    echo "Workflow eq_type=$type done."
  done
done

wait $JOB
