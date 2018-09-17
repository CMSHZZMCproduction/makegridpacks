import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from jhugenjhugenmcsample import JHUGenJHUGenMCSample

class JHUGenJHUGenAnomCoupMCSample(AnomalousCouplingMCSample, JHUGenJHUGenMCSample):
  @property
  def productioncard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "2017", "13TeV", "anomalouscouplings", self.productionmode+"_NNPDF31_13TeV")
    if os.path.exists(os.path.join(folder, "makecards.py")):
      makecards(folder)
#    print folder
    cardbase = self.productionmode
    #card = os.path.join(folder, cardbase+"_NNPDF31_13TeV_M{:d}.input".format(self.mass))
    card = os.path.join(folder, self.kind + ".input")

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def productioncardusesscript(self):
    return False

  @property
  def tarballversion(self):
    v = 1
    return v

  @property
  def timepereventqueue(self):
    return "1nw"


  def cvmfstarball_anyversion(self, version):
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V7011", self.productionmode+"_ZZ_NNPDF31_13TeV")

    tarballname = self.datasetname+".tgz"

    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(version), tarballname)

  @property
  def doublevalidationtime(self):
    return self.productionmode in ("ZH", "ttH")

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
  def fragmentname(self):
    if self.productionmode == "ttH":
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"
    elif self.productionmode in ("VBF", "HJJ", "ZH", "WH"):
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    raise ValueError("No fragment for {}".format(self))

  @classmethod
  def allsamples(cls):
    for productionmode in "HJJ", "VBF", "ZH","WH","ttH" :
    #for productionmode in "HJJ", "VBF"  :
        decaymode = "4l" 
        for mass in cls.getmasses(productionmode, decaymode) :
            for kind in cls.getkind(productionmode, decaymode) :
                yield cls(2017, productionmode, decaymode, mass, kind)

  @property
  def responsible(self):
     return "skeshri"
