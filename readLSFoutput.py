#!/usr/bin/env python

from makegridpacks import *

for stdout in glob.glob("LSFJOB_*/STDOUT"):
  with open(stdout) as f:
    for line in f:
      match = re.match("(.*) size and time per event are found to be (.*) and (.*), will send it to McM", line.strip())
      if match:
        sample = POWHEGJHUGenMassScanMCSample(*match.group(1).split())
        sample.sizeperevent = float(match.group(2))
        sample.timeperevent = float(match.group(3))
