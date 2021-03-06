import abc, contextlib, csv, os, re

from utilities import cache, genproductions, urlopen

from mcsamplebase import MCSampleBase

class AnomalousCouplingMCSample(MCSampleBase):
  def __init__(self, year, productionmode, decaymode, mass,coupling):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.coupling = coupling
    super(AnomalousCouplingMCSample, self).__init__(year=year)
  @property
  def initargs(self):
    return self.year, self.productionmode, self.decaymode, self.mass, self.coupling
  @property
  def identifiers(self):
    return self.productionmode, self.decaymode, self.mass, self.coupling
  @property
  def xsec(self): return 1 #unknown for unknown signal
  @property
  def decaycard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

    couplings = ["L1","L1Zg","L1Zgmix","L1mix","a2","a2mix","a3","a3mix"]

    if self.decaymode == "4l" :
        if self.productionmode in ("ZH", "ggZH") :
        #if self.productionmode in ("ZH") :
            decaymode = "ZZ2l2any_withtaus_filter4l"
        elif self.productionmode == "ttH" :
            decaymode = "ZZ2l2any_withtaus_filter4lOSSF"
        else :
            decaymode = "ZZ4l_withtaus"

        if self.coupling == "SM" or self.productionmode in ("HJJ", "ttH") :
            filename = decaymode+".input"
        elif self.productionmode == "ggZH" and ( self.coupling == "box" or self.coupling == "triangle" ):
            filename = decaymode+".input"

        else :
            if "mix" not in self.coupling or self.productionmode == "ggH":
                filename = "anomalouscouplings/"+decaymode+"_"+self.coupling+".input"
            else :
                filename = "anomalouscouplings/"+decaymode+"_"+self.coupling+"for"+self.productionmode+".input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @classmethod
  @cache
  def csvfile2016(cls):
    with contextlib.closing(urlopen("https://raw.githubusercontent.com/CJLST/ZZAnalysis/f7d5b5fecf322a8cffa435cfbe3f05fb1ae6aba2/AnalysisStep/test/prod/samples_2016_MC_anomalous.csv")) as f:
      return list(f)

  @property
  def datasetname(self):
    result = {
      "ggH": "Higgs",
      "VBF": "VBFHiggs",
      "HJJ": "JJHiggs",
      "ZH":  "ZHiggs",
      "WH":  "WHiggs",
      "ttH": "ttHiggs",
      #"ggZH": "ggZHiggs"
      "ggZH": "GluGluToZHiggs"
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
      "box": "0PBOX", #### ggZH
      "triangle": "0PTRI" #### ggZH
    }[self.coupling]

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
    result += "_JHUGen"+self.JHUGenversion.upper().replace(".", "")+"_pythia8"

    pm = self.productionmode.replace("HJJ", "JJH").replace("H", "Higgs").replace("ggHiggs", "Higgs").replace("ggZHiggs", "GluGluToZHiggs")
    dm = self.decaymode.upper().replace("NU", "Nu")
    searchfor = [pm, dm, "M{:d}".format(self.mass), "JHUGen"+self.JHUGenversion.upper().replace(".", "")+"_"]
    shouldntbethere = []
    if isinstance(self, POWHEGJHUGenMCSample):
      searchfor.append("powheg")
    else:
      shouldntbethere.append("powheg")
    if any(_ not in result for _ in searchfor) or any(_.lower() in result.lower() for _ in shouldntbethere):
      raise ValueError("Dataset name doesn't make sense:\n{}\n{}\nNOT {}\n{}".format(result, searchfor, shouldntbethere, self))

    searchfor = re.sub("JHUGenV[0-9]+", "JHUgenV6", result.replace("Zg", ""))
    reader = csv.DictReader(self.csvfile2016())
    #for row in reader:
    #  if row["dataset"] and row["dataset"].split("/")[1] == searchfor:
    #    break
    #else:
    #  raise ValueError("Couldn't find dataset name {}".format(searchfor))

    if self.productionmode == "ggZH": result = result.replace("13TeV", "TuneCP5_13TeV")

    return result

  @property
  def nevents(self):
    if self.decaymode == "4l":
      if self.productionmode in ("HJJ", "ttH"):
        if self.year == 2016 and self.productionmode == "HJJ": return 1500000 - 250000
        return 250000
      elif self.productionmode in ("ggH", "VBF", "WH"):
        return 500000
      elif self.productionmode == "ZH":
        return 750000
      elif self.productionmode == "ggZH":
        return 750000  #####?

    raise ValueError("No nevents for {}".format(self))

  @classmethod
  def getmasses(cls, productionmode, decaymode):
    if decaymode == "4l":
      if productionmode == "ggH" or productionmode == "HJJ" or productionmode == "VBF" or productionmode == "ZH" or productionmode == "WH" or productionmode == "ttH" or productionmode == "ggZH":
        return 125,
    raise ValueError("No masses for {} {}".format(productionmode, decaymode))

  @classmethod
  def getcouplings(cls,productionmode,decaymode):
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
    if productionmode == "ggZH":
      return "L1", "L1Zg", "L1Zgmix", "L1mix", "SM", "box", "triangle"
