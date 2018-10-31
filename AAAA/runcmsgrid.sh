#!/bin/bash

nevt=${1}
echo "%MSG-MG5 number of events requested = $nevt"

rnum=${2}
echo "%MSG-MG5 random seed used for the run = $rnum"

ncpu=${3}
echo "%MSG-MG5 number of cpus = $ncpu"

LHEWORKDIR=`pwd`
use_gridpack_env=true
if [ -n "$4" ]
  then
  use_gridpack_env=$4
fi

if [ "$use_gridpack_env" = true ]
  then
    if [ -n "$5" ]
      then
        scram_arch_version=${5}
      else
        scram_arch_version=slc6_amd64_gcc630
    fi
    echo "%MSG-MG5 SCRAM_ARCH version = $scram_arch_version"

    if [ -n "$6" ]
      then
        cmssw_version=${6}
      else
        cmssw_version=CMSSW_9_3_0
    fi
    echo "%MSG-MG5 CMSSW version = $cmssw_version"
    export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
    source $VO_CMS_SW_DIR/cmsset_default.sh
    export SCRAM_ARCH=${scram_arch_version}
    scramv1 project CMSSW ${cmssw_version}
    cd ${cmssw_version}/src
    eval `scramv1 runtime -sh`
fi

set -euo pipefail

cd $LHEWORKDIR

if [ -d JHUGenMELA ]; then
  eval $(JHUGenMELA/MELA/setup.sh env)
fi

cd JHUGenerator/

./JHUGen $(cat ../JHUGen.input) VegasNc2=${nevt} Seed=${rnum} DataFile=undecayed &&
./JHUGen $(cat ../JHUGen_decay.input) Seed=${rnum} ReadLHE=undecayed.lhe Seed=${rnum} DataFile=Out

mv Out.lhe $LHEWORKDIR/cmsgrid_final.lhe
cd $LHEWORKDIR
