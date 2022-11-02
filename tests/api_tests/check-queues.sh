#!/bin/sh
set -eu

# CHECK QUEUES SH
# Check that the EMEWS Queues are empty before running a test

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/../.. )
export EQ_SQL
export PYTHONPATH=$EQ_SQL/python

python3 $THIS/check-queues.py
