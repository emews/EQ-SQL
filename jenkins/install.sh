#!/bin/zsh
set -eu

# JENKINS INSTALL
# Test that we can install the EQ-SQL stack
# Can be tested outside Jenkins, simply set
# environment variable WORKSPACE to the EQ-SQL clone
# Provide -m to skip the Miniconda (re-)installation

if [[ ${WORKSPACE:-} == "" ]] {
  print "Set WORKSPACE!"
  return 1
}
cd $WORKSPACE

renice --priority 19 --pid $$

zparseopts -D -E m=M

install-miniconda()
{
  # The Miniconda we are working with:
  MINICONDA=Miniconda3-py311_24.11.1-0-Linux-x86_64.sh

  # Clean up prior runs
  rm -fv $MINICONDA
  rm -fr $WORKSPACE/sfw/Miniconda

  (
    # Download and install Miniconda:
    set -x
    wget  https://repo.anaconda.com/miniconda/$MINICONDA  # --no-verbose
    bash $MINICONDA -b -p $WORKSPACE/sfw/Miniconda
  )
}

if (( ${#M} == 0 )) install-miniconda

PATH=$WORKSPACE/sfw/Miniconda/bin:$PATH

set -x
which conda
PKGS=(
  swift-t
  postgresql
  psycopg2
)
conda install --yes -c swift-t -c conda-forge $PKGS
# conda install -c swift-t swift-t-r

cd python
pip install -e .
cd -

if [[ -d emews-project-creator ]]
then
  rm -rf emews-project-creator
fi
git clone https://github.com/emews/emews-project-creator.git
cd emews-project-creator
pip install -e .
