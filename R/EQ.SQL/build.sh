#!/bin/sh
set -eu

THIS=$( readlink --canonicalize $( dirname $0 ) )

renice -n 19 $$

cd $THIS
Rscript -e "devtools::document()"
cd ..

R CMD build   EQ.SQL
R CMD INSTALL EQ.SQL
