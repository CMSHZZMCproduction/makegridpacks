import contextlib, csv, os, re, subprocess

from utilities import cache, cacheaslist, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from mcsamplebase import MCSampleBase_DefaultCampaign
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class POWHEGJHUGenLifetimeMCSample(POWHEGJHUGenMCSample, MCSampleBase_DefaultCampaign):
  def __init__(self, year, productionmode, decaymode, mass, lifetime):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.lifetime = int(str(lifetime))
    super(POWHEGJHUGenLifetimeMCSample, self).__init__(year=year)
  @property
  def identifiers(self):
    return self.productionmode, self.decaymode, self.mass, "lifetime", self.lifetime

  @property
  def xsec(self): return 1 #unknown for unknown signal
 
  @property
  def powhegprocess(self):
    return POWHEGJHUGenMassScanMCSample(self.year, self.productionmode, self.decaymode, self.mass).powhegprocess

  @property
  def powhegsubmissionstrategy(self): return "onestep"

  @property
  def powhegcard(self):
    return POWHEGJHUGenMassScanMCSample(self.year, self.productionmode, self.decaymode, self.mass).powhegcard

  @property
  def decaycard(self):
    decaymode = os.path.basename(POWHEGJHUGenMassScanMCSample(self.year, self.productionmode, self.decaymode, 
self.mass).decaycard).replace(".input", "")
    return os.path.join(genproductions, "bin/JHUGen/cards/decay/lifetime", "{}_CTau{}um.input".format(decaymode, 
self.lifetime))

  @property
  def powhegcardusesscript(self): return True

  @property
  def tarballversion(self):
    v = 1

    v += 1 #wrong powheg process
    v += 1 #lifetime units

    return v

  def cvmfstarball_anyversion(self, version):
    folder = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V7011/lifetime"
    if version == 1 and self.productionmode == "ggH" and self.decaymode == "4l" and self.mass == 125:
      tarballname = "ZZ_slc6_amd64_gcc630_CMSSW_9_3_0_ZZ4l_lifetime_NNPDF31_13TeV_{}um.tgz".format(self.lifetime)
      return os.path.join(folder, tarballname)
    tarballname = self.datasetname+".tgz"
    if version <= 3: tarballname = tarballname.replace("JHUGen", "JHUgen")
    return os.path.join(folder, tarballname.replace(".tgz", "").replace("JHUGen", "JHUgen"), "v{}".format(version), 
tarballname)

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
    return "c6b09012caef889ecc4d11de26a3718ef7f9d242"

  @property
  def nfinalparticles(self):
    if self.productionmode == "ggH": return 1
    raise ValueError("No fragment for {}".format(self))

  @classmethod
  @cacheaslist
  def allsamples(cls):
    for lifetime in 50, 200, 800:
      yield cls(2017, "ggH", "4l", 125, lifetime)

  @property
  def responsible(self):
     return "hroskes"

  @property
  def nevents(self): return 500000

  @property
  def JHUGenversion(self):
    if self.year in (2017, 2018):
      return "v7.0.11"
    assert False, self

  @property
  def hasnonJHUGenfilter(self): return False

