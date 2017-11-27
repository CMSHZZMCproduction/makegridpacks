#!/usr/bin/env python

from makegridpacks import *

if __name__ == "__main__":
  with RequestQueue() as queue:
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2nu", "2l2q":
        for mass in getmasses(productionmode, decaymode):
          sample = MCSample(productionmode, decaymode, mass)
          if sample.needsupdate and sample.prepid and os.path.exists(sample.cvmfstarball):
            print sample
            queue.addrequest(sample, useprepid=True)

