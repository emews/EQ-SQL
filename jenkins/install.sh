#!/bin/bash
set -eu

# JENKINS INSTALL
# Test that we can install the EQ-SQL stack
# Can be tested outside Jenkins, simply set
# environment variable WORKSPACE to the EQ-SQL clone

renice --priority 19 --pid $$

# The Miniconda we are working with:
MINICONDA=Miniconda3-py39_23.3.1-0-Linux-x86_64.sh

# Clean up prior runs
# rm -fv $MINICONDA
# rm -fr $WORKSPACE/sfw/Miniconda

# (
#   # Download and install both Minicondas:
#   set -x
#   wget --no-verbose https://repo.anaconda.com/miniconda/$MINICONDA
#   bash $MINICONDA -b -p $WORKSPACE/sfw/Miniconda
# )

PATH=$WORKSPACE/sfw/Miniconda/bin:$PATH

set -x
which conda
conda install -c swift-t -c conda-forge swift-t
# conda install -c swift-t swift-t-r
conda install postgres

# Development pip install (pip install -e) eqsql python packages
pip install -e .

git clone https://github.com/emews/emews-project-creator.git
cd emews-project-creator
pip install -e .
