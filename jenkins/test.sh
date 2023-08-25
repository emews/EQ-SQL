#!/bin/bash
set -eu

# TEST SH
# Run this after ./install.sh

set -x
EMEWS_INSTALL=$( cd $WORKSPACE/../EMEWS-Install ; pwd -P )
PATH=$EMEWS_INSTALL/sfw/Miniconda/bin:$PATH

set -x
which emewscreator
emewscreator --help
emewscreator init_db

# Test using this workflow:

git clone https://github.com/emews/emews_examples
