import contextlib, csv, os, re, subprocess

from utilities import cache, cacheaslist, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from mcsamplebase import Run2MCSampleBase
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSampleRun2

class POWHEGJHUGenAnomCoupMCSample(AnomalousCouplingMCSample, POWHEGJHUGenMCSample):
  @property
  def powhegprocess(self):
    if self.productionmode == "ggH": return "gg_H_quark-mass-effects"
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def powhegcardusesscript(self): return True

  @property
  def powhegsubmissionstrategy(self): return "multicore"

  @property
  def tarballversion(self):
    v = 1
    return v

  def cvmfstarball_anyversion(self, version):
    if self.year in (2017, 2018):
      folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2", self.powhegprocess+"_ZZ_NNPDF31_13TeV", "anomalouscouplings")
      tarballname = self.datasetname+".tgz"
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(version), tarballname)

  @property
  def foldernameforrunpwg(self):
    return super(POWHEGJHUGenAnomCoupMCSample, self).foldernameforrunpwg+"_"+self.coupling

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
    if self.year == 2017:
      return "fd7d34a91c3160348fd0446ded445fa28f555e09"
    if self.year == 2018:
      return "f256d395f40acf771f12fd6dbecd622341e9731a"
    assert False, self.year

  @property
  def nfinalparticles(self):
    if self.productionmode == "ggH": return 1
    raise ValueError("No fragment for {}".format(self))

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
  def makegridpackseed(self):
    result = super(POWHEGJHUGenAnomCoupMCSample, self).makegridpackseed
    if self.productionmode == "ggH" and self.decaymode == "4l" and self.coupling == "L1Zg" and self.multicore_upto[0] == 2: result = -5347775664555457122
    if self.productionmode == "ggH" and self.decaymode == "4l" and self.coupling == "L1mix" and self.multicore_upto[0] == 2: result = -5347775664555457122
    if self.productionmode == "ggH" and self.decaymode == "4l" and self.coupling == "SM" and self.multicore_upto[0] == 2: result = -5347775664555457122
    if self.productionmode == "ggH" and self.decaymode == "4l" and self.coupling == "a2mix" and self.multicore_upto == (1, 5): result = -5347775664555457122
    return result

class POWHEGJHUGenAnomCoupMCSampleRun2(POWHEGJHUGenAnomCoupMCSample, Run2MCSampleBase):
  @property
  def powhegcard(self):
    return POWHEGJHUGenMassScanMCSampleRun2(self.year, self.productionmode, self.decaymode, self.mass).powhegcard

  @classmethod
  @cacheaslist
  def allsamples(cls):
    for productionmode in "ggH", :
      decaymode = "4l"
      for mass in cls.getmasses(productionmode, decaymode):
        for coupling in cls.getcouplings(productionmode, decaymode):
          for year in 2017, 2018:
            yield cls(year, productionmode, decaymode, mass, coupling)

