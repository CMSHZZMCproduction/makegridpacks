import os

from utilities import genproductions

from powhegmcsample import POWHEGMCSample

class QQZZMCSample(POWHEGMCSample):
  def __init__(self, finalstate, cut=None):
    self.finalstate = finalstate
    self.cut = cut
  @property
  def identifiers(self):
    result = ["qqZZ", self.finalstate]
    if self.cut: result.append(self.cut)
    return tuple(result)
  @property
  def powhegprocess(self): return "ZZ"
  @property
  def powhegcard(self):
    folder = os.path.join(genproductions, "bin", "Powheg", "production", "2017", "13TeV", "ZZ")
    if self.finalstate == "4l":
      if self.cut is None: filename = "ZZ_4L_NNPDF31_13TeV.input"
      elif self.cut == "100-160": filename = "ZZ_4L_100-160GeV_NNPDF31_13TeV.input"
    elif self.finalstate == "2l2nu":
      if self.cut is None: filename = "ZZ_2L2NU_NNPDF31_13TeV.input"
    try:
      return os.path.join(folder, filename)
    except NameError:
      raise ValueError("No powheg card for {}".format(self))
  @property
  def powhegcardusesscript(self): return False
  @property
  def powhegsubmissionstrategy(self): return "multicore"
  @property
  def creategridpackqueue(self): return ("1nw" if self.cut else "1nh") if super(QQZZMCSample, self).creategridpackqueue is not None else None
  @property
  def tarballversion(self):
    v = 1
    if self.cut is None: v += 1
    elif self.cut == "100-160": pass
    else: assert 0, repr(self.cut)
    return v

  @property
  def cvmfstarball(self):
    maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/"
    if self.finalstate == "2l2nu":
      folder = os.path.join(maindir, "ZZTo2L2NU")
      if self.cut is None: filename = "ZZ_slc6_amd64_gcc630_CMSSW_9_3_0_ZZTo2L2NU2017_pdf306000.tgz"
    elif self.finalstate == "4l":
      folder = os.path.join(maindir, "ZZTo4L")
      if self.cut is None: filename = "ZZ_slc6_amd64_gcc630_CMSSW_9_3_0_ZZTo4L2017_pdf306000.tgz"
      elif self.cut == "100-160":
        folder = os.path.join(folder, "100-160GeV")
        filename = "ZZTo4L_100-160GeV.tgz"

    try:
      return os.path.join(folder, "v{}".format(self.tarballversion), filename)
    except NameError:
      assert False, self
  @property
  def datasetname(self):
    if self.finalstate == "4l":
      if self.cut is None: return "ZZTo4L_13TeV_powheg_pythia8"
      elif self.cut == "100-160": return "ZZTo4L_mZZ100-160GeV_13TeV_powheg_pythia8"
    elif self.finalstate == "2l2nu":
      if self.cut is None: return "ZZTo2L2Nu_13TeV_powheg_pythia8"
    assert False, self
  @property
  def defaulttimeperevent(self): return 15
  @property
  def genproductionscommit(self): return "0774506836218eb536d48380d3129cdedefe8f58"
  @property
  def hasfilter(self): return False #the mass cut filter is done within powheg
  @property
  def nevents(self):
    if self.finalstate == "4l" and self.cut is None: return 7000000
    if self.finalstate == "2l2nu" and self.cut is None: return 9000000
    if self.finalstate == "4l" and self.cut == "100-160": return 10000000
    assert False, self
  @property
  def tags(self):
    result = ["HZZ"]
    if self.finalstate in ("4l", "2l2nu") and self.cut is None:
      result.append("Fall17P1C")
      result.append("HTT")
    if self.finalstate == "4l" and self.cut == "100-160":
      result.append("Fall17P3")
    return result
  @property
  def xsec(self):
    assert False, "need to fill this"

  @classmethod
  def allsamples(cls):
    yield cls("4l", "100-160")
    yield cls("4l")
  @property
  def responsible(self):
    if self.finalstate == "4l" and self.cut == "100-160": return "hroskes"
    if self.finalstate == "4l" and self.cut is None: return "hroskes"
    assert False, self
