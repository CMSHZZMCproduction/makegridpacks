#!/bin/bash

set -euo pipefail

for a in {1..1}; do
  waitids=
  for b in {1..2}; do
    if [ $b -gt 1 ]; then
      waitids="$(bjobs -J gridpacks_${a}_$(expr $b - 1) | waitids.py)"
    fi
    echo "cd $(pwd) && $cmsenv && ./makegridpacks.py" | bsub -q 1nd -J gridpacks_${a}_${b} -w "$waitids"
  done
done

i=0
while ! [ -f /cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/HZJ_NNPDF31_13TeV/HZJ_HanythingJ_NNPDF31_13TeV_M145/v3/HZJ_HanythingJ_NNPDF31_13TeV_M145.tgz ]; do
  echo $i
  i=$(expr $i + 1)
  sleep 1m
done

for a in {2..101}; do
  waitids=
  for b in {1..2}; do
    if [ $b -gt 1 ]; then
      waitids="$(bjobs -J gridpacks_${a}_$(expr $b - 1) | waitids.py)"
    fi
    echo "cd $(pwd) && $cmsenv && ./makegridpacks.py" | bsub -q 1nd -J gridpacks_${a}_${b} -w "$waitids"
  done
done
