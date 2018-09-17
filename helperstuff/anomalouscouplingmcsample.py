import abc, contextlib, csv, os, urllib

from utilities import genproductions

from mcsamplebase import MCSampleBase_DefaultCampaign

class AnomalousCouplingMCSample(MCSampleBase_DefaultCampaign):
  def __init__(self, year, productionmode, decaymode, mass,kind):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.kind = kind
    super(AnomalousCouplingMCSample, self).__init__(year=year)
  @property
  def identifiers(self):
    return self.productionmode, self.decaymode, self.mass, self.kind
  @property
  def xsec(self): return 1 #unknown for unknown signal
  @property
  def decaycard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

    couplings = ["L1","L1Zg","L1Zgmix","L1mix","a2","a2mix","a3","a3mix"]

    if self.decaymode == "4l" :
        if self.productionmode == "ZH" :
            decaymode = "ZZ2l2any_withtaus_filter4l"
        elif self.productionmode == "ttH" :
            decaymode = "ZZ2l2any_withtaus_filter4lOSSF"
        else :
            decaymode = "ZZ4l_withtaus"

        if self.kind == "SM" or self.productionmode in ("HJJ", "ttH") :
            filename = decaymode+".input"

        else :
            if "mix" not in self.kind or self.productionmode == "ggH":
                filename = "anomalouscouplings/"+decaymode+"_"+self.kind+".input"
            else :
                filename = "anomalouscouplings/"+decaymode+"_"+self.kind+"for"+self.productionmode+".input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def datasetname(self):
    result = {
      "ggH": "Higgs",
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

    from powhegjhugenmcsample import POWHEGJHUGenMCSample

    result += "_M125_13TeV"
    if isinstance(self, POWHEGJHUGenMCSample):
      result += "_powheg2"
    result += "_JHUGenV7011_pythia8"

    pm = self.productionmode.replace("HJJ", "JJH").replace("H", "Higgs").replace("ggHiggs", "Higgs")
    dm = self.decaymode.upper().replace("NU", "Nu")
    searchfor = [pm, dm, "M{:d}".format(self.mass), "JHUGenV7011_"]
    shouldntbethere = []
    if isinstance(self, POWHEGJHUGenMCSample):
      searchfor.append("powheg")
    else:
      shouldntbethere.append("powheg")
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
  def nevents(self):
    if self.decaymode == "4l":
      if self.productionmode in ("HJJ", "ttH"):
        return 250000
      elif self.productionmode in ("ggH", "VBF", "WH"):
        return 500000
      elif self.productionmode == "ZH":
        return 750000

    raise ValueError("No nevents for {}".format(self))

  @classmethod
  def getmasses(cls, productionmode, decaymode):
    if decaymode == "4l":
      if productionmode == "ggH" or productionmode == "HJJ" or productionmode == "VBF" or productionmode == "ZH" or productionmode == "WH" or productionmode == "ttH":
        return 125,
    raise ValueError("No masses for {} {}".format(productionmode, decaymode))

  @classmethod
  def getkind(cls,productionmode,decaymode):
    if productionmode == "ggH" :
      return "L1","L1Zg","L1Zgmix","L1mix","SM","a2","a2mix","a3","a3mix" 
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
