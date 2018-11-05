#!/bin/bash

set -euo pipefail

cd $1
shift
eval $(scram ru -sh)
./makegridpacks.py "$@"
