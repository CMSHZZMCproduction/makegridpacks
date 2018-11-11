import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from anomalouscouplingmcsample import AnomalousCouplingMCSample
from jhugenjhugenmcsample import JHUGenJHUGenMCSample

class JHUGenJHUGenAnomCoupMCSample(AnomalousCouplingMCSample, JHUGenJHUGenMCSample):
  @property
  def productioncard(self):
    if self.year in (2017, 2018):
      folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "2017", "13TeV", "anomalouscouplings", self.productionmode+"_NNPDF31_13TeV")
    elif self.year == 2016:
      folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "pre2017", "anomalouscouplings", self.productionmode+"_NNPDF30_13TeV")
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
    if self.year in (2017, 2018):
      folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V7011", self.productionmode+"_ZZ_NNPDF31_13TeV")
    elif self.year == 2016:
      folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc481/13TeV/jhugen/V723", self.productionmode+"_ZZ_NNPDF30_13TeV")

    tarballname = self.datasetname+".tgz"

    if self.year == 2016 and self.productionmode == "HJJ" and self.decaymode == "4l" and self.mass == 125 and self.tarballversion == 1:
      tarballname = tarballname.replace("V723", "V7011")

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
    result = ["HZZ"]
    if self.year == 2017: result.append("Fall17P2A")
    return result

  @property
  def genproductionscommit(self):
    if self.year == 2016:
      return "ed512ae283cc2d8710e72ecf37c2ae6cd663aee6"
    if self.year == 2017:
      return "fd7d34a91c3160348fd0446ded445fa28f555e09"
    if self.year == 2018:
      return "f256d395f40acf771f12fd6dbecd622341e9731a"
    assert False, self

  @property
  def fragmentname(self):
    if self.year in (2017, 2018):
      if self.productionmode == "ttH":
        return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"
      elif self.productionmode in ("VBF", "HJJ", "ZH", "WH", "ggZH"):
        return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    if self.year == 2016:
      if self.productionmode == "ttH":
        return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCUETP8M1_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"
      elif self.productionmode in ("VBF", "HJJ", "ZH", "WH", "ggZH"):
        return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCUETP8M1_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    raise ValueError("No fragment for {}".format(self))

  @classmethod
  def allsamples(cls):
    for productionmode in "HJJ", "VBF", "ZH","WH","ttH" :
    #for productionmode in "HJJ", "VBF"  :
      decaymode = "4l" 
      for mass in cls.getmasses(productionmode, decaymode):
        for kind in cls.getkind(productionmode, decaymode):
          for year in 2016, 2017, 2018:
            if year == 2016 and productionmode != "HJJ": continue
            yield cls(year, productionmode, decaymode, mass, kind)

  @property
  def responsible(self):
     return "hroskes"

  @property
  def JHUGenversion(self):
    if self.year in (2017, 2018): return "v7.0.11"
    if self.year == 2016: return "v7.2.3"
    assert False, self

  @property
  def hasnonJHUGenfilter(self): return False

  @property
  def maxallowedtimeperevent(self):
    if self.productionmode in ("VBF", "HJJ"): return 205
    return super(JHUGenJHUGenAnomCoupMCSample, self).maxallowedtimeperevent
