import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class MINLOMCSample(POWHEGJHUGenMCSample):
  def __init__(self, decaymode, mass):
    self.decaymode = decaymode
    self.mass = int(str(mass))

  @property
  def identifiers(self):
    return "MINLO", self.decaymode, self.mass

  @property
  def xsec(self): return 1 #unknown for unknown signal

  @property
  def powhegprocess(self):
    return "HJJ"
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def powhegsubmissionstrategy(self):
    return "multicore"

  def createtarball(self):
    return "making a minlo tarball is not automated, you have to make it yourself and put it in {}".format(self.foreostarball)

  @property
  def powhegcard(self):
    return os.path.join(genproductions, "bin/Powheg/production/2017/13TeV/Higgs/HJJ_NNPDF31_13TeV/HJJ_NNPDF31_13TeV_M{}.input".format(self.mass))

  @property
  def powhegcardusesscript(self): return False

  @property
  def decaycard(self):
    return POWHEGJHUGenMassScanMCSample("ggH", self.decaymode, self.mass).decaycard

  @property
  def timepereventqueue(self): return "1nw"

  @property
  def tarballversion(self):
    v = 1
    if self.mass == 125:  1
    return v

  @property
  def cvmfstarball(self):
    result = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/HJJ_M125_13TeV/v{}/HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_13TeV_M125.tgz".format(self.tarballversion)
    if self.tarballversion == 1 and self.mass==125: result = result.replace("/v1/", "/") 
    return result

  @property
  def datasetname(self):
    return "GluGluHToZZTo4L_M%d_13TeV_powheg2_minloHJJ_JHUGenV7011_pythia8"%(self.mass)

  @property
  def nevents(self):
    return 3000000

  @property
  def defaulttimeperevent(self):
    return 300
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P2A"]

  @property
  def genproductionscommit(self):
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def nfinalparticles(self):
    return 3
    raise ValueError("No fragment for {}".format(self))

  @property
  def doublevalidationtime(self): return True

  @classmethod
  def allsamples(cls):
    for mass in 125, 300:
        for decaymode in "4l",:
            yield cls(decaymode, mass)

  @property
  def responsible(self):
     return "wahung"
