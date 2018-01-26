import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class POWHEGJHUGenLifetimeMCSample(POWHEGJHUGenMCSample):
  def __init__(self, productionmode, decaymode, mass, lifetime):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.lifetime = int(str(lifetime))
  @property
  def identifiers(self):
    return self.productionmode, self.decaymode, self.mass, "lifetime", self.lifetime
 
  @property
  def powhegprocess(self):
    return POWHEGJHUGenMassScanMCSample(self.productionmode, self.decaymode, self.mass).powhegprocess

  @property
  def powhegcard(self):
    return POWHEGJHUGenMassScanMCSample(self.productionmode, self.decaymode, self.mass).powhegcard

  @property
  def decaycard(self):
    decaymode = os.path.basename(POWHEGJHUGenMassScanMCSample(self.productionmode, self.decaymode, self.mass).decaycard).replace(".input", "")
    return os.path.join(genproductions, "bin/JHUGen/cards/decay/lifetime", "{}_CTau{}um.input".format(decaymode, self.lifetime))

  @property
  def powhegcardusesscript(self): return True

  @property
  def queue(self):
    return "1nd"

  @property
  def tarballversion(self):
    v = 1

    v += 1 #wrong powheg process

    return v

  @property
  def cvmfstarball(self):
    folder = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V7011/lifetime"
    if self.tarballversion == 1 and self.productionmode == "ggH" and self.decaymode == "4l" and self.mass == 125:
      tarballname = "ZZ_slc6_amd64_gcc630_CMSSW_9_3_0_ZZ4l_lifetime_NNPDF31_13TeV_{}um.tgz".format(self.lifetime)
      return os.path.join(folder, tarballname)
    tarballname = self.datasetname+".tgz"
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(self.tarballversion), tarballname)

  @property
  def datasetname(self):
    assert self.productionmode == "ggH" and self.decaymode == "4l"
    return "Higgs0PMToZZTo4L_M{}_CTau{}um_13TeV_JHUGenV7011_pythia8".format(self.mass, self.lifetime)

  @property
  def defaulttimeperevent(self):
    return 30
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P2A"]

  @property
  def genproductionscommit(self):
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def nfinalparticles(self):
    if self.productionmode == "ggH": return 1
    raise ValueError("No fragment for {}".format(self))

  @classmethod
  def allsamples(cls):
    for lifetime in 50, 200, 800:
      yield cls("ggH", "4l", 125, lifetime)

  @property
  def responsible(self):
     return "wahung"

  @property
  def nevents(self): return 500000
