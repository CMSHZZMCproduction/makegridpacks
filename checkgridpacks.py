#!/usr/bin/env python

from makegridpacks import *

if __name__ == "__main__":
  for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
    for decaymode in "4l", "2l2nu", "2l2q":
      for mass in getmasses(productionmode, decaymode):
        sample = POWHEGJHUGenMassScanMCSample(productionmode, decaymode, mass)
        if os.path.exists(sample.cvmfstarball):
          try:
            sample.cardsurl
          except Exception as e:
            etext = str(e).replace(str(sample), "").strip()
            print sample, etext
