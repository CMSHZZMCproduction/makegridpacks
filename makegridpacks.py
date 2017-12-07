#!/usr/bin/env python

import os, sys, urllib

from helperstuff.requestqueue import RequestQueue
from helperstuff.powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

def makegridpacks():
  with RequestQueue() as queue:
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2nu", "2l2q":
        for mass in getmasses(productionmode, decaymode):
          sample = POWHEGJHUGenMassScanMCSample(productionmode, decaymode, mass)
          print sample, sample.makegridpack(queue)
          sys.stdout.flush()

if __name__ == "__main__":
  makegridpacks()
