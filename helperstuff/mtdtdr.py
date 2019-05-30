import os

from gridpackonly import POWHEGGridpackOnly
from utilities import cacheaslist, genproductions

class MTDTDRSample(POWHEGGridpackOnly):
  def __init__(self, year, productionmode):
    self.productionmode = productionmode
    super(MTDTDRSample, self).__init__(year)
  @property
  def initargs(self): return self.year, self.productionmode
  @property
  def identifiers(self):
    return "MTDTDR", self.productionmode
  @classmethod
  @cacheaslist
  def allsamples(cls):
    yield cls(2018, "VBF")
    yield cls(2018, "ZH")

  @property
  def cmsswversion(self):
    return "CMSSW_10_4_0_mtd4"
  @property
  def scramarch(self):
    return "slc6_amd64_gcc700"

  def cvmfstarball_anyversion(self, version):
    if self.productionmode == "VBF":
      return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc{}/14TeV/powheg/V2/VBF_H_NNPDF30_M125/v{}/VBF_H_slc6_amd64_gcc630_CMSSW_9_3_0_test_AllInOneVBF_H_NNPDF30_14TeV_M125.tgz".format(630 if version==1 else 700, version)
    if self.productionmode == "ZH":
      return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc{}/14TeV/powheg/V2/HZJ_HanythingJ_NNPDF30_M125/v{}/HZJ_slc6_amd64_gcc630_CMSSW_9_3_0_test_AllInOneHZJ_HanythingJ_NNPDF30_14TeV_M125_Vleptonic.tgz".format(630 if version==1 else 700, version)
    assert False, self
  @property
  def tarballversion(self):
    v = 1
    if self.productionmode in ("VBF", "ZH"): v+=1
    return v

  @property
  def genproductionscommit(self):
    if self.productionmode == "VBF":
      return "cbd9f361d33cfe95b76668588168103e986126ff"
    if self.productionmode == "ZH":
      return "51d57507cb4efb9b4554beb636d97219f63da6b8"
    assert False, self
  @property
  def powhegcard(self):
    if self.productionmode == "VBF":
      return os.path.join(genproductions, "bin/Powheg/production/V2/13TeV/Higgs/VBF_H_NNPDF30_13TeV/VBF_H_NNPDF30_13TeV_M-125.input")
    if self.productionmode == "ZH":
      return os.path.join(genproductions, "bin/Powheg/production/VH_from_Hbb/HZJ_HanythingJ_NNPDF30_13TeV_M125_Vleptonic.input")
    assert False, self
  @property
  def powhegcardusesscript(self):
    return False
  @property
  def powhegprocess(self):
    if self.productionmode == "VBF": return "VBF_H"
    if self.productionmode == "ZH": return "HZJ"
    assert False, self
  @property
  def powhegsubmissionstrategy(self):
    if self.productionmode == "ZH": return "multicore"
    if self.productionmode == "VBF": return "onestep"
    assert False, self
  @property
  def responsible(self):
    return "hroskes"
  @property
  def defaultnthreads(self): return 8
