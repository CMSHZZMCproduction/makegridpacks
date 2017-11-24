#!/bin/bash

set -euo pipefail

for a in {1..100}; do
  waitids=
  for b in {1..1}; do
    if [ $b -gt 1 ]; then
      waitids="$(bjobs -J gridpacks_${a}_$(expr $b - 1) | waitids.py)"
    fi
    echo "cd $(pwd) && $cmsenv && ./makegridpacks.py" | bsub -q 1nd -J gridpacks_${a}_${b} -w "$waitids"
  done
done
