#!/bin/sh
set -eu

# DEPS SH
# Simply run deps.R

THIS=$( readlink --canonicalize $( dirname $0 ) )

which R

Rscript $THIS/deps.R
