# Environment for swift-t commit 9ad37bb: 
# * python_parallel_persist support
# 
# STC: Swift-Turbine Compiler 0.8.3
#          for Turbine: 1.2.3
# Using Java VM:    /blues/gpfs/home/software/spack-0.10.1/opt/spack/linux-centos7-x86_64/gcc-7.1.0/jdk-11.0.2_9-bacc5fdgi74fwnv74aigbvprtpdatkgv/bin/java
# Using Turbine in: /lcrc/project/EMEWS/bebop/sfw/swift-t-9ad37bb/turbine

# Turbine 1.2.3
#  installed:    /lcrc/project/EMEWS/bebop/sfw/swift-t-9ad37bb/turbine
#  source:       /lcrc/project/EMEWS/bebop/repos/swift-t/turbine/code
#  using CC:     /blues/gpfs/home/software/spack-0.10.1/opt/spack/linux-centos7-x86_64/gcc-7.1.0/mvapich2-2.3a-avvw4kp72uach6daxfe6kbm3yrvedh5a/bin/mpicc
#  using MPI:    /gpfs/fs1/home/software/spack-0.10.1/opt/spack/linux-centos7-x86_64/gcc-7.1.0/mvapich2-2.3a-avvw4kp72uach6daxfe6kbm3yrvedh5a/lib mpi "MPICH"
#  using Tcl:    /gpfs/fs1/home/software/spack-0.10.1/opt/spack/linux-centos7-x86_64/gcc-7.1.0/tcl-8.6.8-blvvcoseerw2hsb3d5fhezczx3mjgxy2/bin/tclsh8.6
#  using Python: /lcrc/project/EMEWS/bebop/sfw/anaconda3/2020.11/lib python3.8
#  using R:      /lcrc/project/EMEWS/bebop/repos/spack/opt/spack/linux-centos7-broadwell/gcc-7.1.0/r-4.0.0-plchfp7jukuhu5oity7ofscseg73tofx/rlib/R


module load gcc/7.1.0-4bgguyp 
module load mvapich2/2.3a-avvw4kp
module unload intel-mkl/2018.1.163-4okndez
. /lcrc/project/EMEWS/bebop/repos/spack/share/spack/setup-env.sh
# r 4.0.0
spack load /plchfp7
spack load intel-mkl@2020.1.217
module load anaconda3/2020.11

export R_LIBS=/lcrc/project/EMEWS/bebop/repos/spack/opt/spack/linux-centos7-broadwell/gcc-7.1.0/r-4.0.0-plchfp7jukuhu5oity7ofscseg73tofx/rlib/R/library/
# spack loading R puts another python into the path, so we set the correct one here.
export PATH=/lcrc/project/EMEWS/bebop/sfw/swift-t-9ad37bb/stc/bin:/soft/anaconda3/2020.11/bin:$PATH
export PYTHONHOME=/lcrc/project/EMEWS/bebop/sfw/anaconda3/2020.11

