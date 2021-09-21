#!/bin/bash
set -eu

# POOLS 1

THIS=$(   readlink --canonicalize $( dirname $0 ) )
EQ_SQL=$( readlink --canonicalize $THIS/.. )
export EQ_SQL

Rscript $THIS/test-pools-1-me.R

wait
