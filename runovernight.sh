#!/bin/bash

set -euo pipefail

(echo $STY) || (echo "run this on a screen"; exit 1)

! (echo $CMSSW_BASE) || (echo "run this in a new session, without doing cmsenv"; exit 1)

(eval $(scram ru -sh); echo $CMSSW_BASE) || (echo "run this inside a CMSSW release (but don't cmsenv)"; exit 1)

for i in {1..192}; do (
  source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh
  eval $(scram ru -sh)
  ./makegridpacks.py
) || true
sleep 15m
done
