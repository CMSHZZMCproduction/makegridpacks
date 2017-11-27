#!/bin/bash

set -euo pipefail

for a in {201..300}; do
  waitids=
  for b in {1..4}; do
    if [ $b -gt 1 ]; then
      waitids="$(bjobs -J gridpacks_${a}_$(expr $b - 1) | waitids.py)"
    fi
    echo "cd $(pwd) && $cmsenv && ./makegridpacks.py" | bsub -q 1nd -J gridpacks_${a}_${b} -w "$waitids"
  done
done
