import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from mcsamplebase import MCSampleBase_DefaultCampaign
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class MINLOMCSample(POWHEGJHUGenMCSample, MCSampleBase_DefaultCampaign):
  def __init__(self, year, decaymode, mass, energy=13):
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.energy = int(str(energy))
    super(MINLOMCSample, self).__init__(year=year)

  @property
  def identifiers(self):
    result = ["MINLO", self.decaymode, self.mass]
    if self.energy != 13: result.append(str(self.energy)+"TeV")
    return tuple(result)

  @property
  def makegridpackcommand(self):
    result = super(MINLOMCSample, self).makegridpackcommand
    if self.energy == 14:
      result += ["-d", "1"]
    return result

  @property
  def pwgrwlfilter(self):
    def filter(weight):
      if self.energy == 13:
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
  def creategridpackqueue(self):
    if super(MINLOMCSample, self).creategridpackqueue is None: return None
    if self.multicore_upto[0] == 0: return "1nh"
    if self.multicore_upto[0] in (2, 3): return "1nw"
    return "1nd"

  def createtarball(self):
    if self.energy == 13:
      return "making a minlo tarball is not automated, you have to make it yourself and put it in {}".format(self.foreostarball)
    return super(MINLOMCSample, self).createtarball()

  @property
  def powhegcard(self):
    if self.energy == 13:
      return os.path.join(genproductions, "bin/Powheg/production/2017/13TeV/Higgs/HJJ_NNPDF31_13TeV/HJJ_NNPDF31_13TeV_M{}.input".format(self.mass))
    elif self.energy == 14:
      return os.path.join(genproductions, "bin/Powheg/production/pre2017/14TeV/HJJ_NNPDF30_14TeV/HJJ_NNPDF30_14TeV_M{mass}.input".format(mass=self.mass))

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
    if self.mass == 125 and self.energy == 14 and self.decaymode == "4l":  v+=1  #remove some pdfs
    if self.mass == 125 and self.energy == 14 and self.decaymode == "4l":  v+=1  #remove some more pdfs
    if self.mass == 125 and self.energy == 14 and self.decaymode == "4l":  v+=1  #remove ALL pdfs
    if self.mass == 125 and self.energy == 13 and self.decaymode == "4l":  v+=1  #remove some pdfs
    if self.mass == 300 and self.energy == 13 and self.decaymode == "4l":  v+=1  #remove some pdfs
    return v

  def cvmfstarball_anyversion(self, version):
    if self.energy == 13: year = "2017"
    if self.energy == 14: year = "slc6_amd64_gcc481"
    repmap = dict(version=version, mass=self.mass, energy=self.energy, year=year)

    maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/{year}/{energy}TeV/powheg/V2"
    if self.energy == 13:
      folder = "HJJ_M{mass}_{energy}TeV"
    elif self.energy == 14:
      folder = "HJJ_HZZ4L_NNPDF30_14TeV_M125_JHUGenV710"
    else: assert False

    if self.energy == 13 and (version == 1 or self.mass == 125 and self.decaymode == "4l" and version == 2):
      tarballname = "HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_{energy}TeV_M{mass}.tgz"
    else:
      tarballname = folder+".tgz"

    return os.path.join(maindir, folder, "v{version}", tarballname).format(**repmap)

  @property
  def datasetname(self):
    return "GluGluHToZZTo4L_M%d_%dTeV_powheg2_minloHJJ_JHUGenV7011_pythia8"%(self.mass, self.energy)

  @property
  def campaign(self):
    if self.energy == 13:
      return super(MINLOMCSample, self).campaign
    if self.energy == 14:
      if year == 2017: return "PhaseIISummer17wmLHEGENOnly"
    assert False

  @property
  def nevents(self):
    if self.energy == 14: return 1000000
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
    return "1383619647949814646806a6fc8b0ecd3228f293"

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
            yield cls(2017, decaymode, mass)
    yield cls(2017, "4l", 125, energy=14)

  @property
  def responsible(self):
     return "hroskes"
