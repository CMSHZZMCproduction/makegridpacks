#!/bin/bash

set -euo pipefail

for a in $(find . -type d | tac); do
  if ! ls $a/* >& /dev/null; then
    rmdir $a
  fi
done
