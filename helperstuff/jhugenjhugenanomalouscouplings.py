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
  def queue(self):
    return "1nd"

  @property
  def tarballversion(self):
    v = 1

    return v

  @property
  def cvmfstarball(self):
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V7011", self.productionmode+"_ZZ_NNPDF31_13TeV")

    tarballname = self.datasetname+".tgz"

    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(self.tarballversion), tarballname)

  @property
  def datasetname(self):
    result = {
      "VBF": "VBFHiggs",
      "HJJ": "JJHiggs",
      "ZH":  "ZHiggs",
      "WH":  "WHiggs",
      "ttH": "ttHiggs",
    }[self.productionmode]
    result += {
      "SM": "0PM",
      "a2": "0PH",
      "a3": "0M",
      "0M": "0M",  #for ttH
      "L1": "0L1",
      "L1Zg": "0L1Zg",
      "a2mix": "0PHf05ph0",
      "a3mix": "0Mf05ph0",
      "0Mmix": "0Mf05ph0",  #for ttH
      "L1mix": "0L1f05ph0",
      "L1Zgmix": "0L1Zgf05ph0",
    }[self.kind]

    result += "ToZZ"
    if "ZZ4l_withtaus" in self.decaycard:
      result += "To4L"
    elif "ZZ2l2any_withtaus_filter4l" in self.decaycard:
      result += "_4LFilter"
    else:
      raise ValueError("Unknown decay card\n"+self.decaycard)

    result += "_M125_13TeV_JHUGenV7011_pythia8"

    pm = self.productionmode.replace("HJJ", "JJH").replace("H", "Higgs")
    dm = self.decaymode.upper().replace("NU", "Nu")
    searchfor = [pm, dm, "M{:d}".format(self.mass), "JHUGenV7011_"]
    shouldntbethere = ["powheg"]
    if any(_ not in result for _ in searchfor) or any(_.lower() in result.lower() for _ in shouldntbethere):
      raise ValueError("Dataset name doesn't make sense:\n{}\n{}\nNOT {}\n{}".format(result, searchfor, shouldntbethere, self))

    searchfor = result.replace("Zg", "").replace("JHUGenV7011", "JHUgenV6")
    with contextlib.closing(urllib.urlopen("https://raw.githubusercontent.com/CJLST/ZZAnalysis/f7d5b5fecf322a8cffa435cfbe3f05fb1ae6aba2/AnalysisStep/test/prod/samples_2016_MC_anomalous.csv")) as f:
      reader = csv.DictReader(f)
      for row in reader:
        if row["dataset"] and row["dataset"].split("/")[1] == searchfor:
          break
      else:
        raise ValueError("Couldn't find dataset name {}".format(searchfor))

    return result

  @property
  def defaulttimeperevent(self):
    return 30
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P2A"]

  @property
  def genproductionscommit(self):
    return "fb46462ba79b16eef88cbc03a738e6c3dbf22773"

  @property
  def fragmentname(self):
    if self.productionmode == "ttH":
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"
    elif self.productionmode in ("VBF", "HJJ", "ZH", "WH"):
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    raise ValueError("No fragment for {}".format(self))

  @classmethod
  def getmasses(cls, productionmode, decaymode):
    if decaymode == "4l":
      if productionmode == "HJJ" or productionmode == "VBF" or productionmode == "ZH" or productionmode == "WH" or productionmode == "ttH":
        return 125

  @classmethod
  def getkind(cls,productionmode,decaymode):
    if productionmode == "HJJ" :
      return "SM","a3","a3mix" 
    if productionmode == "VBF" :
      return "L1","L1Zg","L1Zgmix","L1mix","SM","a2","a2mix","a3","a3mix" 
    if productionmode == "WH" :
      return "L1","L1mix","SM","a2","a2mix","a3","a3mix" 
    if productionmode == "ZH" :
      return "L1","L1Zg","L1Zgmix","L1mix","SM","a2","a2mix","a3","a3mix" 
    if productionmode == "ttH" :
      return "0M","0Mmix","SM" 


  @classmethod
  def allsamples(cls):
    for productionmode in "HJJ", "VBF", "ZH","WH","ttH" :
    #for productionmode in "HJJ", "VBF"  :
        decaymode = "4l" 
        mass = cls.getmasses(productionmode, decaymode) 
        for kind in cls.getkind(productionmode, decaymode) :
            yield cls(productionmode, decaymode, mass, kind)

  @property
  def responsible(self):
     return "skeshri"
