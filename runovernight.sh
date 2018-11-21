#!/bin/bash

set -euo pipefail

(echo $STY) || (echo "run this on a screen"; exit 1)

! (echo $CMSSW_BASE) || (echo "run this in a new session, without doing cmsenv"; exit 1)

(eval $(scram ru -sh); echo $CMSSW_BASE) || (echo "run this inside a CMSSW release (but don't cmsenv)"; exit 1)

for i in {1..192}; do (
  echo -n "it's currently "; date
  echo "running for time ${i}/192"
  source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh
  eval $(scram ru -sh)
  ./makegridpacks.py "$@"
  bswitch -q 1nh cmscaf1nh 0
  bswitch -q 1nd cmscaf1nd 0
  bswitch -q 1nw cmscaf1nw 0
  echo -n "it's currently "; date
  echo "will run again in 15 minutes, which will be time $((${i}+1))/192"
) || true
sleep 15m || true
done
