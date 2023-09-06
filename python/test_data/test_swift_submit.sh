#! /usr/bin/env bash

set -eu

if [ "$#" -ne 2 ]; then
  script_name=$(basename $0)
  echo "Usage: ${script_name} exp_id cfg_file"
  exit 1
fi

export TURBINE_LOG=0 TURBINE_DEBUG=0 ADLB_DEBUG=0
# export TURBINE_STDOUT=out-%%r.txt
export TURBINE_STDOUT=
export ADLB_TRACE=0
export EMEWS_PROJECT_ROOT=$( cd $( dirname $0 )/.. ; /bin/pwd )
# source some utility functions used by EMEWS in this script                                                                                 
# source "${EMEWS_PROJECT_ROOT}/swift-t/ext/emews_utils.sh"

export EXPID=$1
export TURBINE_OUTPUT=$EMEWS_PROJECT_ROOT/experiments/$EXPID
# check_directory_exists

CFG_FILE=$2
source $CFG_FILE

echo "--------------------------"
# echo "WALLTIME:              $CFG_WALLTIME"
echo "PROCS:                 $CFG_PROCS"
echo "PPN:                   $CFG_PPN"
echo "--------------------------"

export TURBINE_LAUNCHER=srun
export PROJECT=$CFG_PROJECT
export QUEUE=$CFG_QUEUE
export WALLTIME=$CFG_WALLTIME

export PROCS=$CFG_PROCS
export PPN=$CFG_PPN
export TURBINE_JOBNAME="${EXPID}_job"

export PATH=/lcrc/project/EMEWS/bebop/sfw/swift-t-9ad37bb/stc/bin:$PATH

swift-t -m slurm -i sys -E 'sleep(600) => trace("DONE");'
