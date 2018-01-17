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
    return POWHEGJHUGenMassScanMCSample(self.productionmode, self.decaymode, self.mass).powhegcard

  @property
  def queue(self):
    return "1nd"

  @property
  def tarballversion(self):
    v = 1

    return v

  @property
  def cvmfstarball(self):
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2", self.powhegprocess+"_ZZ_NNPDF31_13TeV", "anomalouscouplings")
    tarballname = self.datasetname
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(self.tarballversion), tarballname)

  @property
  def defaulttimeperevent(self):
    return 30
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P2A"]

  @property
  def genproductionscommit(self):
    return "ee94dea404c5b05c9805ad74d42aab506223fbf2"

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
                yield cls(productionmode, decaymode, mass, kind)

  @property
  def responsible(self):
     return "skeshri"
