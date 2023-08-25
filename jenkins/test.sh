#!/bin/bash
set -eu

# TEST SH
# Run this after ./install.sh

set -x
EMEWS_INSTALL=$( cd $WORKSPACE/../EMEWS-Install ; pwd -P )
PATH=$EMEWS_INSTALL/sfw/Miniconda/bin:$PATH
PATH=$EMEWS_INSTALL/db:$PATH

set -x
DB=$WORKSPACE/DB
which emewscreator
emewscreator --help
emewscreator init_db --db_path $DB

db-start.sh $DB

db-stop.sh $DB

# Test using this workflow:
git clone https://github.com/emews/emews_examples
