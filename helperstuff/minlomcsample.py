import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class MINLOMCSample(POWHEGJHUGenMCSample):
  def __init__(self, decaymode, mass, energy=13):
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.energy = int(str(energy))

  @property
  def identifiers(self):
    result = ["MINLO", self.decaymode, self.mass]
    if self.energy != 13: result.append(str(self.energy)+"TeV")
    return tuple(result)

  @property
  def xsec(self): return 1 #unknown for unknown signal

  @property
  def powhegprocess(self):
    return "HJJ"
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def powhegsubmissionstrategy(self):
    return "multicore"

  @property
  def creategridpackqueue(self):
    if super(MINLOMCSample, self).creategridpackqueue is None: return None
    if self.multicore_upto[0] in (2, 3): return "1nw"
    return "1nh"

  def createtarball(self):
    if self.energy == 13:
      return "making a minlo tarball is not automated, you have to make it yourself and put it in {}".format(self.foreostarball)
    return super(MINLOMCSample, self).createtarball()

  @property
  def powhegcard(self):
    if self.energy == 13:
      return os.path.join(genproductions, "bin/Powheg/production/2017/13TeV/Higgs/HJJ_NNPDF31_13TeV/HJJ_NNPDF31_13TeV_M{}.input".format(self.mass))
    elif self.energy == 14:
      return os.path.join(genproductions, "bin/Powheg/production/pre2017/14TeV/HJJ_NNPDF30_14TeV/HJJ_NNPDF30_14TeV_M{mass}.input".format(mass=self.mass)

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
    if self.mass == 125 and self.energy == 13 and self.decaymode == "4l":  v+=1
    return v

  @property
  def cvmfstarball(self):
    result = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/{energy}TeV/powheg/V2/HJJ_M125_{energy}TeV/v{}/HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_{energy}TeV_M125.tgz".format(self.tarballversion, energy=self.energy)
    if self.tarballversion == 1 and self.mass==125 and self.energy == 13 and self.decaymode == "4l": result = result.replace("/v1/", "/") 
    return result

  @property
  def datasetname(self):
    return "GluGluHToZZTo4L_M%d_%dTeV_powheg2_minloHJJ_JHUGenV7011_pythia8"%(self.mass, self.energy)

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
