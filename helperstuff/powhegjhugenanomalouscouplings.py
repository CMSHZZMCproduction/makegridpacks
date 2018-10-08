import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from powhegjhugenmcsample import POWHEGJHUGenMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class POWHEGJHUGenAnomCoupMCSample(AnomalousCouplingMCSample, POWHEGJHUGenMCSample):
  @property
  def powhegprocess(self):
    if self.productionmode == "ggH": return "gg_H_quark-mass-effects"
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def powhegcard(self):
    return POWHEGJHUGenMassScanMCSample(self.year, self.productionmode, self.decaymode, self.mass).powhegcard

  @property
  def powhegcardusesscript(self): return True

  @property
  def powhegsubmissionstrategy(self): return "onestep"

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
    return super(POWHEGJHUGenAnomCoupMCSample, self).foldernameforrunpwg+"_"+self.kind

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
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def nfinalparticles(self):
    if self.productionmode == "ggH": return 1
    raise ValueError("No fragment for {}".format(self))

  @classmethod
  def allsamples(cls):
    for productionmode in "ggH", :
      decaymode = "4l"
      for mass in cls.getmasses(productionmode, decaymode):
        for kind in cls.getkind(productionmode, decaymode):
          for year in 2017, 2018:
            yield cls(year, productionmode, decaymode, mass, kind)

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

