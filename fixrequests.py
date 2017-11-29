#!/usr/bin/env python

import argparse

from makegridpacks import *

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--dry-run", "-n", action="store_true", help="don't send anything to McM")
  parser.add_argument("--times", "-t", action="store_true", help="get the times from McM for all requests")
  args = parser.parse_args()

  with RequestQueue() as queue:
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2nu", "2l2q":
        for mass in getmasses(productionmode, decaymode):
          sample = MCSample(productionmode, decaymode, mass)
          if (sample.needsupdate or args.times) and sample.prepid and os.path.exists(sample.cvmfstarball):
            sample.gettimepereventfromMcM()
            print sample
            if sample.needsupdate and not args.dry_run:
              queue.addrequest(sample, useprepid=True)

