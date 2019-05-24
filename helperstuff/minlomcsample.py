import contextlib, csv, os, re, subprocess

from utilities import cache, cacheaslist, cd, genproductions, here, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from mcsamplebase import MCSampleBase, Run2MCSampleBase
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class MINLOMCSample(POWHEGJHUGenMCSample):
  def __init__(self, year, decaymode, mass):
    self.decaymode = decaymode
    self.mass = int(str(mass))
    super(MINLOMCSample, self).__init__(year=year)

  @property
  def initargs(self): return self.year, self.decaymode, self.mass

  @property
  def identifiers(self):
    result = ["MINLO", self.decaymode, self.mass]
    return tuple(result)

  @property
  def pwgrwlfilter(self):
    def filter(weight):
      if weight.pdfname.startswith("NNPDF31_"): return True
      if weight.pdfname.startswith("NNPDF30_"): return True
      if weight.pdfname.startswith("PDF4LHC15"): return True
      return False
    return filter

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
  def powhegcard(self):
    return os.path.join(genproductions, "bin/Powheg/production/2017/13TeV/Higgs/HJJ_NNPDF31_13TeV/HJJ_NNPDF31_13TeV_M{}.input".format(self.mass))

  @property
  def powhegcardusesscript(self): return False

  @property
  def decaycard(self):
    return POWHEGJHUGenMassScanMCSample(self.year, "ggH", self.decaymode, self.mass).decaycard

  @property
  def timepereventqueue(self): return "nextweek"

  @property
  def tarballversion(self):
    v = 1
    if self.year in (2017, 2018):
      if self.mass == 125 and self.decaymode == "4l":  v+=1
      if self.mass == 125 and self.decaymode == "4l":  v+=1  #remove some pdfs
      if self.mass == 300 and self.decaymode == "4l":  v+=1  #remove some pdfs
      if self.mass == 300 and self.decaymode == "4l":  v+=1  #parallelize
      if self.mass == 300 and self.decaymode == "4l":  v+=1  #parallelize with xargs
    if self.year == 2018:
      if self.mass == 125 and self.decaymode == "4l":  v+=2  #parallelize with xargs
    return v

  @property
  def patchkwargs(self):
    result = super(MINLOMCSample, self).patchkwargs
    result.append({"functionname": "parallelizepowheg"})
    return result

  def cvmfstarball_anyversion(self, version):
    year = "2017"
    repmap = dict(version=version, mass=self.mass, year=year)

    maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/{year}/13TeV/powheg/V2"
    folder = "HJJ_M{mass}_13TeV"

    if version == 1 or self.mass == 125 and self.decaymode == "4l" and version == 2:
      tarballname = "HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_13TeV_M{mass}.tgz"
    else:
      tarballname = folder+".tgz"

    return os.path.join(maindir, folder, "v{version}", tarballname).format(**repmap)

  @property
  def datasetname(self):
    return "GluGluHToZZTo4L_M%d_13TeV_powheg2_minloHJJ_JHUGenV7011_pythia8"%(self.mass)

  @property
  def nevents(self):
    return 3000000

  @property
  def defaulttimeperevent(self):
    return 30
    assert False

  @property
  def tags(self):
    result = ["HZZ"]
    if self.year == 2017: result.append("Fall17P2A")
    return result

  @property
  def genproductionscommit(self):
    return "1383619647949814646806a6fc8b0ecd3228f293"

  @property
  def genproductionscommitforfragment(self):
    if self.year == 2018: return "20f59357146e08e48132cfd73d0fd72ca08b6b30"
    return super(MINLOMCSample, self).genproductionscommitforfragment

  @property
  def nfinalparticles(self):
    return 3
    raise ValueError("No fragment for {}".format(self))

  @property
  def validationtimemultiplier(self): return max(super(MINLOMCSample, self).validationtimemultiplier, 2)

  @property
  def responsible(self):
     return "hroskes"

  @property
  def JHUGenversion(self):
    if self.year in (2017, 2018):
      return "v7.0.11"
    assert False, self

  @property
  def hasnonJHUGenfilter(self): return False

  @property
  def maxallowedtimeperevent(self):
    return 500

class MINLOMCSamplePhaseII(MINLOMCSample):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    yield cls(2017, "4l", 125)
  @property
  def identifiers(self):
    result = super(MINLOMCSamplePhaseII, self).identifiers
    result = result + ("14TeV",)
    return result
  @property
  def makegridpackcommand(self):
    result = super(MINLOMCSamplePhaseII, self).makegridpackcommand
    if self.energy == 14:
      result += ["-d", "1"]
    return result
  @property
  def pwgrwlfilter(self):
    return lambda weight: False
  @property
  def powhegcard(self):
    return os.path.join(genproductions, "bin/Powheg/production/pre2017/14TeV/HJJ_NNPDF30_14TeV/HJJ_NNPDF30_14TeV_M{mass}.input".format(mass=self.mass))
  @property
  def tarballversion(self):
    v = 1
    assert self.year == 2017, self
    if self.mass == 125 and self.decaymode == "4l":  v+=1  #remove some pdfs
    if self.mass == 125 and self.decaymode == "4l":  v+=1  #remove some more pdfs
    if self.mass == 125 and self.decaymode == "4l":  v+=1  #remove ALL pdfs
    return v

  def cvmfstarball_anyversion(self, version):
    year = "slc6_amd64_gcc481"
    repmap = dict(version=version, mass=self.mass, year=year)

    maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/{year}/14TeV/powheg/V2"
    folder = "HJJ_HZZ4L_NNPDF30_14TeV_M125_JHUGenV710"

    tarballname = folder+".tgz"

    return os.path.join(maindir, folder, "v{version}", tarballname).format(**repmap)

  @property
  def datasetname(self):
    return "GluGluHToZZTo4L_M%d_14TeV_powheg2_minloHJJ_JHUGenV7011_pythia8"%(self.mass)

  @property
  def campaign(self):
    if self.year == 2017: return "PhaseIISummer17wmLHEGENOnly"
    assert False

  @property
  def nevents(self):
    return 1000000

  @property
  def tags(self):
    return ["HZZ"]

class MINLOMCSampleRun2(MINLOMCSample, Run2MCSampleBase):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    for mass in 125, 300:
      for decaymode in "4l",:
        for year in 2017, 2018:
          yield cls(year, decaymode, mass)
