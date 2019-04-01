#!/bin/bash

set -euo pipefail

(echo $STY) || (echo "run this on a screen"; exit 1)

for i in {1..192}; do (
  echo -n "it's currently "; date
  echo "running for time ${i}/192"
  ./makegridpacks.py "$@"
  echo -n "it's currently "; date
  echo "will run again in 15 minutes, which will be time $((${i}+1))/192"
) || true
sleep 15m || true
done
