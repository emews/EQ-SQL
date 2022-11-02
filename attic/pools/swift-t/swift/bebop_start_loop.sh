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
source "${EMEWS_PROJECT_ROOT}/etc/emews_utils.sh"

export EXPID=$1
export TURBINE_OUTPUT=$EMEWS_PROJECT_ROOT/experiments/$EXPID
check_directory_exists

CFG_FILE=$2
source $CFG_FILE

echo "--------------------------"
echo "WALLTIME:              $CFG_WALLTIME"
echo "PROCS:                 $CFG_PROCS"
echo "PPN:                   $CFG_PPN"
echo "DB_HOST:               $CFG_DB_HOST"
echo "DB_USER:               $CFG_DB_USER"
echo "TASK_TYPE:             $CFG_TASK_TYPE"
echo "--------------------------"

export PROCS=$CFG_PROCS
export QUEUE=$CFG_QUEUE
export WALLTIME=$CFG_WALLTIME
export PPN=$CFG_PPN
export TURBINE_JOBNAME="${EXPID}_job"
export PROJECT=$CFG_PROJECT

export DB_HOST=$CFG_DB_HOST
export DB_USER=$CFG_DB_USER
export DB_PORT=$CFG_DB_PORT

# if R cannot be found, then these will need to be
# uncommented and set correctly.
# export R_HOME=/path/to/R
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$R_HOME/lib
# if python packages can't be found, then uncommited and set this
# PYTHONPATH="/lcrc/project/EMEWS/bebop/repos/probabilistic-sensitivity-analysis:"
# PYTHONPATH+="/lcrc/project/EMEWS/bebop/repos/panmodel-0.20.0:"
# PYTHONPATH+="$EMEWS_PROJECT_ROOT/python"
# export PYTHONPATH
# echo "PYTHONPATH: $PYTHONPATH"
EQ_SQL=$( readlink --canonicalize $EMEWS_PROJECT_ROOT/../.. )
PYTHONPATH=$EQ_SQL/db:$EQ_SQL/python
echo "PYTHONPATH: $PYTHONPATH"

# export SITE=bebop

# Resident task workers and ranks
# export TURBINE_RESIDENT_WORK_WORKERS=1
# export RESIDENT_WORK_RANKS=$(( PROCS - 2 ))

# EQ/R location
# EQR=/lcrc/project/EMEWS/bebop/repos/spack/opt/spack/linux-centos7-broadwell/gcc-7.1.0/eqr-1.0-5hb4aszbbtezlifks6fz4g24zldnkdbx
EQ=$EMEWS_PROJECT_ROOT/../../swift
EMEWS_EXT=$EMEWS_PROJECT_ROOT/ext/emews


# set machine to your schedule type (e.g. pbs, slurm, cobalt etc.),
# or empty for an immediate non-queued unscheduled run
MACHINE="slurm"

if [ -n "$MACHINE" ]; then
  MACHINE="-m $MACHINE"
fi

mkdir -p $TURBINE_OUTPUT
cp $CFG_FILE $TURBINE_OUTPUT/cfg.cfg

CFG_EXTRA_FILES_TO_INCLUDE=${CFG_EXTRA_FILES_TO_INCLUDE:-}
for f in ${CFG_EXTRA_FILES_TO_INCLUDE[@]}; do
  tf="$(basename -- $f)"
  cp $EMEWS_PROJECT_ROOT/$f $TURBINE_OUTPUT/$tf
done

CMD_LINE_ARGS="$*"


# Add any script variables that you want to log as
# part of the experiment meta data to the USER_VARS array,
# for example, USER_VARS=("VAR_1" "VAR_2")
USER_VARS=("MODEL_DIR" "STOP_AT" "MODEL_PROPS" \
 "STOP_AT")
# log variables and script to to TURBINE_OUTPUT directory

export TURBINE_LAUNCHER=srun
# export TURBINE_SBATCH_ARGS="-c 18"

PG_LIB=/lcrc/project/EMEWS/bebop/sfw/gcc-7.1.0/postgres-14.2/lib
MKL=/lcrc/project/EMEWS/bebop/repos/spack/opt/spack/linux-centos7-broadwell/gcc-7.1.0/intel-mkl-2020.1.217-dqzfemzfucvgn2wdx7efg4swwp6zs7ww
MKL_LIB=$MKL/mkl/lib/intel64
MKL_OMP_LIB=$MKL/lib/intel64
LDP=$MKL_LIB/libmkl_def.so:$MKL_LIB/libmkl_avx2.so:$MKL_LIB/libmkl_core.so:$MKL_LIB/libmkl_intel_lp64.so:$MKL_LIB/libmkl_intel_thread.so:$MKL_OMP_LIB/libiomp5.so
log_script

# echo's anything following this standard out
# set -x

swift-t -n $PROCS $MACHINE -p \
    -r $EQ -I $EQ \
    -I $EMEWS_EXT \
    -e EMEWS_PROJECT_ROOT \
    -e TURBINE_OUTPUT \
    -e TURBINE_LOG \
    -e TURBINE_DEBUG \
    -e ADLB_DEBUG \
    -e LD_LIBRARY_PATH=$MKL_LIB:$PG_LIB:$LD_LIBRARY_PATH \
    -e LD_PRELOAD=$LDP \
    -e DB_HOST \
    -e DB_USER \
    -e DB_PORT \
    -e PYTHONPATH \
    $EMEWS_PROJECT_ROOT/swift/loop.swift $CMD_LINE_ARGS

chmod g+rw $TURBINE_OUTPUT/*.tic
