import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from massscanmcsample import MassScanMCSample
from jhugenjhugenmcsample import JHUGenJHUGenMCSample

class JHUGenJHUGenMassScanMCSample(MassScanMCSample, JHUGenJHUGenMCSample):
  @property
  def productioncard(self):
    if self.year in (2017, 2018):
      yearfolder = "2017"
      pdf = "NNPDF31"
    elif self.year == 2016:
      yearfolder = "pre2017"
      pdf = "NNPDF30"
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", yearfolder, "13TeV", self.productionmode+"_"+pdf+"_13TeV")
    if os.path.exists(os.path.join(folder, "makecards.py")):
      makecards(folder)

    cardbase = self.productionmode
    card = os.path.join(folder, cardbase+"_"+pdf+"_13TeV_M{:d}.input".format(self.mass))

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def productioncardusesscript(self):
    if self.productionmode == "tqH": return False
    if self.productionmode == "bbH": return True
    if self.productionmode == "ggZH": return False
    assert False, self

  @property
  def reweightdecay(self):
    return False

  @property
  def filter4L(self):
    if self.productionmode == "ggZH": return True
    return False

  @property
  def tarballversion(self):
    v = 1

    if self.productionmode == "bbH" and self.decaymode in ("4l", "2l2q"): v += 1 #https://github.com/cms-sw/genproductions/commit/c3b983b4edd969369a26d62d4bcd0d9c6dd1d528
    if self.productionmode == "tqH" and self.decaymode in ("4l", "2l2q") and self.mass == 125: v += 1 #https://github.com/cms-sw/genproductions/commit/c3b983b4edd969369a26d62d4bcd0d9c6dd1d528

    return v

  def cvmfstarball_anyversion(self, version):
    if self.year in (2017, 2018):
      yearfolder = "2017"
      pdf = "NNPDF31"
      filename_ = yearfolder+"/13TeV/jhugen/V7011"
      ##folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V7011", self.productionmode+"_ZZ_NNPDF31_13TeV")
    elif self.year == 2016:
      yearfolder = "pre2017"
      pdf = "NNPDF30"
      filename_ = "slc6_amd64_gcc481"+"/13TeV/jhugen/V7011"
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks", filename_, self.productionmode+"_ZZ_"+pdf+"_13TeV")
    ##folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks", yearfolder, "13TeV/jhugen/V7011", self.productionmode+"_ZZ_"+pdf+"_13TeV")
    tarballname = os.path.basename(self.productioncard).replace(".input", ".tgz")
    decaymode = self.decaymode
    if "ZZ2l2any_withtaus.input" in self.decaycard: decaymode == "2l2X"
    elif "ZZany_filter2lOSSF.input" in self.decaycard: decaymode = "_filter2l"
    elif "ZZ2l2any_withtaus_filter4l.input" in self.decaycard: decaymode = "2l2X_filter4l"
    ##tarballname = tarballname.replace("NNPDF31", "ZZ"+self.decaymode+"_NNPDF31")
    tarballname = tarballname.replace(pdf, "ZZ"+self.decaymode+"_"+pdf)
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(version), tarballname)

  @property
  @cache
  def olddatasetname(self):
    p = self.productionmode
    if p == "VBF": p = "VBFH"
    with contextlib.closing(urllib.urlopen("https://raw.githubusercontent.com/CJLST/ZZAnalysis/miniAOD_80X/AnalysisStep/test/prod/samples_2016_MC.csv")) as f:
      reader = csv.DictReader(f)
      for row in reader:
        if row["identifier"] == "{}{}".format(p, self.mass):
          dataset = row["dataset"]
          result = re.sub(r"^/([^/]*)/[^/]*/[^/]*$", r"\1", dataset)
          assert result != dataset and "/" not in result, result
          if self.decaymode == "4l":
            return result
    raise ValueError("Nothing for {}".format(self))

  @property
  def datasetname(self):
    if self.productionmode == "ggZH" and self.decaymode == "4l" and self.mass == 125:
      return "GluGluToZH_HToZZTo4L_M125_13TeV_JHUGenV723_pythia8"
    if self.decaymode == "2l2nu":
      result = type(self)(self.year, self.productionmode, "4l", self.mass).datasetname.replace("4L", "2L2Nu")
    elif self.decaymode == "2l2q":
      result = type(self)(self.year, self.productionmode, "4l", self.mass).datasetname.replace("4L", "2L2Q")
      if self.mass == 125:
        if self.productionmode in ("bbH", "tqH"): result = result.replace("2L2Q", "2L2X")
    elif self.productionmode in ("bbH", "tqH") and self.mass != 125:
      result = type(self)(self.year, self.productionmode, self.decaymode, 125).datasetname.replace("M125", "M{:d}".format(self.mass))
    else:
      result = self.olddatasetname.replace("JHUgenV702", "JHUGenV7011")

    pm = self.productionmode
    dm = self.decaymode.upper().replace("NU", "Nu")
    if self.decaymode == "2l2q" and self.mass == 125:
      if self.productionmode in ("bbH", "tqH"): dm = "2L2X"
    searchfor = [pm, dm, "M{:d}".format(self.mass), "JHUGen{}_".format(self.JHUGenversion.replace("v", "V").replace(".", ""))]
    shouldntbethere = ["powheg"]
    if any(_ not in result for _ in searchfor) or any(_.lower() in result.lower() for _ in shouldntbethere):
      raise ValueError("Dataset name doesn't make sense:\n{}\n{}\nNOT {}\n{}".format(result, searchfor, shouldntbethere, self))

    return result

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
    if self.productionmode == "ggZH": return "20ac197949817a9bc02aa346f3fe23d157371b74"
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def genproductionscommitforfragment(self):
    if self.productionmode == "ggZH":
      if self.year == 2017:
        return "fd7d34a91c3160348fd0446ded445fa28f555e09"
      elif self.year == 2016:
        return "ef267369910e01ce1eb4f4fabe5b223339829ff5"
    if self.year == 2018:
      return "20ac197949817a9bc02aa346f3fe23d157371b74"
    return super(JHUGenJHUGenMassScanMCSample, self).genproductionscommitforfragment
 
  @property
  def fragmentname(self):
    if self.year in (2017, 2018):
      if self.productionmode in ("bbH", "tqH"): return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"
      if self.productionmode == "ggZH": return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    if self.year == 2016:
      if self.productionmode == "ggZH": return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCUETP8M1_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    ##return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"

  @classmethod
  def getmasses(cls, productionmode, decaymode):
    if decaymode == "4l":
      if productionmode == "bbH":
        return 115, 120, 124, 125, 126, 130, 135, 140, 145
      if productionmode == "tqH":
        return 125,
      if productionmode == "ggZH":
        return 125,
    if decaymode == "2l2q":
      if  productionmode == "ggZH": return ()
      if productionmode in ("bbH", "tqH", "ggZH"):
        return 125,
    if decaymode == "2l2nu":
      if productionmode in ("bbH", "tqH", "ggZH"):
        return ()

  @classmethod
  def allsamples(cls):
    for year in 2017, 2018:
      for productionmode in "bbH", "tqH", "ggZH":
        for decaymode in "4l", "2l2q", "2l2nu":
          for mass in cls.getmasses(productionmode, decaymode):
            yield cls(year, productionmode, decaymode, mass)
    if year == 2016:
      for productionmode == "ggZH":
        for decaymode in "4l", "2l2q", "2l2nu":
          for mass in cls.getmasses(productionmode, decaymode):
            yield cls(year, productionmode, decaymode, mass)

  @property
  def responsible(self):
    if self.productionmode == "ggZH": return "qguo"
    return "hroskes"

  @property
  def nevents(self):
    if self.year == 2017:
      if self.decaymode == "4l":
        if self.productionmode == "bbH":
          if 124 <= self.mass <= 126: return 500000
          return 200000
        elif self.productionmode == "tqH":
          if self.mass == 125: return 1000000
        elif self.productionmode == "ggZH":
          if self.mass == 125: return 1000000
      elif self.decaymode == "2l2q":
        if self.productionmode in ("bbH", "tqH", "ggZH"):
          if self.mass == 125: return 500000
    if self.year == 2018:
      if self.decaymode == "4l":
        if self.productionmode == "bbH":
          if 105 <= self.mass <= 140: return 500000
          return 200000
        elif self.productionmode == "tqH":
          if self.mass == 125: return 1000000
        elif self.productionmode == "ggZH":
          if self.mass == 125: return 1000000
      elif self.decaymode == "2l2q":
        if self.productionmode in ("bbH", "tqH", "ggZH"):
          if self.mass == 125: return 500000
    if self.year == 2016:
      if self.decaymode == "4l":
         if self.productionmode == "ggZH":
           if self.mass == 125: return 1000000
      elif self.decaymode == "2l2q":
        if self.productionmode == "ggZH":
          if self.mass == 125: return 500000


    raise ValueError("No nevents for {}".format(self))

  @property
  def JHUGenversion(self):
    if self.year in (2017, 2018):
      if self.productionmode in ("bbH", "tqH"): return "v7.0.11"
      if self.productionmode == "ggZH": return "v7.2.3"
    if self.year == 2016:
      if self.productionmode == "ggZH": return "v7.2.3"
 
  @property
  def hasnonJHUGenfilter(self): return False

  def linkmela(self):
    if self.productionmode == "ggZH": return True
    return False

  def handle_request_fragment_check_warning(self, line):
    if line.strip() == "* [WARNING] Large time/event - please check":
      if self.timeperevent <= 180 and self.productionmode == "bbH": return "ok"
    return super(JHUGenJHUGenMassScanMCSample, self).handle_request_fragment_check_warning(line)

