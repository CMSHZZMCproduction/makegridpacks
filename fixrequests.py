#!/usr/bin/env python

import argparse

from makegridpacks import *

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--dry-run", "-n", action="store_true")
  args = parser.parse_args()

  with RequestQueue() as queue:
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2nu", "2l2q":
        for mass in getmasses(productionmode, decaymode):
          sample = MCSample(productionmode, decaymode, mass)
          if sample.needsupdate and sample.prepid and os.path.exists(sample.cvmfstarball):
            sample.gettimepereventfromMcM()
            print sample
            if not args.dry_run:
              queue.addrequest(sample, useprepid=True)

