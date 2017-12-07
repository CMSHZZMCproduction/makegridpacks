#!/usr/bin/env python

import os, sys, urllib

from utilities import cache, cd, cdtemp, rm_f, here, jobended, JsonDict, KeepWhileOpenFile, LSB_JOBID, mkdir_p, \
                      mkdtemp, NamedTemporaryFile, restful, TFile, wget

#do not change these once you've started making tarballs!
#they are included in the tarball name and the script
#will think the tarballs don't exist even if they do
cmsswversion = "CMSSW_9_3_0"
scramarch = "slc6_amd64_gcc630"

genproductions = os.path.join(os.environ["CMSSW_BASE"], "src", "genproductions")
if not os.path.exists(genproductions) or os.environ["CMSSW_VERSION"] != cmsswversion or os.environ["SCRAM_ARCH"] != scramarch:
  raise RuntimeError("Need to cmsenv in a " + cmsswversion + " " + scramarch + " release that contains genproductions")

here = os.path.dirname(os.path.abspath(__file__))

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
