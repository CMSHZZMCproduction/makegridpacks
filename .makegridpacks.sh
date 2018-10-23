#!/bin/bash

set -euo pipefail

eval $(scram ru -sh)
./makegridpacks.py
