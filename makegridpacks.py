#!/usr/bin/env python

import csv, glob, os, random, re, shutil, subprocess, sys, urllib

from utilities import cache, cd, KeepWhileOpenFile, LSB_JOBID, mkdir_p, mkdtemp, TFile

#do not change these once you've started making tarballs!
#they are included in the tarball name and the script
#will think the tarballs don't exist even if they do
cmsswversion = "CMSSW_9_3_0"
scramarch = "slc6_amd64_gcc630"

genproductions = os.path.join(os.environ["CMSSW_BASE"], "src", "genproductions")
if not os.path.exists(genproductions) or os.environ["CMSSW_VERSION"] != cmsswversion or os.environ["SCRAM_ARCH"] != scramarch:
  raise RuntimeError("Need to cmsenv in a " + cmsswversion + " " + scramarch + " release that contains genproductions")

here = os.path.dirname(os.path.abspath(__file__))

class MCSample(object):
  def __init__(self, productionmode, mass):
    self.productionmode = productionmode
    self.mass = int(str(mass))

  def __eq__(self, other):
    return self.productionmode == other.productionmode and self.mass == other.mass
  def __ne__(self, other):
    return not (self == other)
  def __hash__(self):
    return hash((self.productionmode, self.mass))
  def __str__(self):
    return "{} {}".format(self.productionmode, self.mass)
  def __repr__(self):
    return "{}({!r}, {!r})".format(type(self).__name__, self.productionmode, self.mass)

  @property
  def powhegprocess(self):
    if self.productionmode == "ggH": return "gg_H_quark-mass-effects"
    if self.productionmode == "VBF": return "VBF_H"
    if self.productionmode == "ZH": return "HZJ"
    if self.productionmode in ("WplusH", "WminusH"): return "HWJ"
    if self.productionmode == "ttH": return "ttH"
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def powhegcard(self):
    folder = os.path.join(genproductions, "bin", "Powheg", "production", "2017", "13TeV", self.powhegprocess+"_NNPDF31_13TeV")
    makecards(folder)

    cardbase = self.powhegprocess
    if self.productionmode == "ZH": cardbase = "HZJ_HanythingJ"
    if self.productionmode == "WplusH": cardbase = "HWplusJ_HanythingJ"
    if self.productionmode == "WminusH": cardbase = "HWminusJ_HanythingJ"
    if self.productionmode == "ttH": cardbase = "ttH_inclusive"
    card = os.path.join(folder, cardbase+"_NNPDF31_13TeV_M{:d}.input".format(self.mass))

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def filter4L(self):
    if self.productionmode in ("ggH", "VBF", "WplusH", "WminusH"): return False
    if self.productionmode in ("ZH", "ttH"): return True
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def reweightdecay(self):
    return self.mass >= 200

  @property
  def JHUGencard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

    filename = "ZZ4l" if self.filter4L else "ZZ2l2any"
    filename += "_withtaus"
    if self.reweightdecay: filename += "_reweightdecay_CPS"
    filename += ".input"

    card = os.path.join(folder, filename)
    
    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def tarballversion(self):
    return 1

  @property
  def cvmfstarball(self):
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2", self.powhegprocess+"_NNPDF31_13TeV")
    tarballname = os.path.basename(self.powhegcard).replace(".input", ".tgz")
    return os.path.join(folder, "v{}".format(self.tarballversion, tarballname))

  @property
  def eostarball(self):
    return self.cvmfstarball.replace("/cvmfs/cms.cern.ch/phys_generator/", "/eos/cms/store/group/phys_generator/cvmfs/")

  @property
  def foreostarball(self):
    """to put in a directory structure here, which will later be copied to eos"""
    return self.cvmfstarball.replace("/cvmfs/cms.cern.ch/phys_generator/", here+"/")

  @property
  def workdir(self):
    return os.path.join()

  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", os.path.basename(self.powhegcard).replace(".input", ""),
             self.powhegprocess+"_"+scramarch+"_"+cmsswversion+"_"+os.path.basename(self.powhegcard).replace(".input", ".tgz"))

  @property
  def queue(self):
    if self.productionmode == "ggH": return "1nh"
    return "2nd"

  @property
  def makegridpackcommand(self):
    args = {
      "-p": "f",
      "-i": self.powhegcard,
      "-g": self.JHUGencard,
      "-m": self.powhegprocess,
      "-f": os.path.basename(self.powhegcard).replace(".input", ""),
      "-q": self.queue,
      "-n": "10",
    }

    return [os.path.join(genproductions, "bin", "Powheg", "run_pwg.py")] + sum(([k, v] for k, v in args.iteritems()), [])

  def makegridpack(self):
    if os.path.exists(self.cvmfstarball): return "exists on cvmfs"
    if os.path.exists(self.eostarball): return "exists on eos, not yet copied to cvmfs"
    if os.path.exists(self.foreostarball): return "exists in this folder, to be copied to eos"

    workdir = os.path.dirname(self.tmptarball)
    mkdir_p(workdir)
    with cd(workdir), KeepWhileOpenFile(self.tmptarball+".tmp", message=LSB_JOBID()) as kwof:
      if not kwof: return "job to make the tarball is already running"

      if not os.path.exists(self.tmptarball):
        if not LSB_JOBID(): return "please run on a queue"
        output = subprocess.check_output(self.makegridpackcommand)
        print output
        waitids = []
        for line in output.split("\n"):
          if "is submitted to" in line:
            waitids.append(int(line.split("<")[1].split(">")[0]))
        assert waitids
        subprocess.check_call(["bsub", "-q", "cmsinter", "-I", "-J", "wait for "+str(self), "-w", " && ".join("ended({})".format(_) for _ in waitids), "echo", "done"])
      mkdir_p(os.path.dirname(self.foreostarball))
      shutil.move(self.tmptarball, self.foreostarball)
      shutil.rmtree(os.path.dirname(self.tmptarball))
      return "tarball is created and moved to this folder, to be copied to eos"

@cache
def makecards(folder):
  with cd(folder):
    subprocess.check_call(["./makecards.py"])

def getmasses(productionmode):
  if productionmode in ("ggH", "VBF", "WplusH", "WminusH", "ZH"):
    return 115, 120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 165, 170, 175, 180, 190, 200, 210, 230, 250, 270, 300, 350, 400, 450, 500, 550, 600, 700, 750, 800, 900, 1000, 1500, 2000, 2500, 3000
  if productionmode == "ttH":
    return 115, 120, 124, 125, 126, 130, 135, 140, 145

def makegridpacks():
  for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
    for mass in getmasses(productionmode):
      sample = MCSample(productionmode, mass)
      print sample, sample.makegridpack()

if __name__ == "__main__":
  makegridpacks()
