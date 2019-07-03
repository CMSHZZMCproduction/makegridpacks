import os

from gridpackbysomeoneelse import MadGraphHZZdFromJake, MadGraphHZZdFromJakeRun2
from madgraphmcsample import MadGraphMCSample
from mcfmanomalouscouplings import MCFMAnomCoupMCSample, MCFMAnomCoupMCSampleRun2
from mcsamplebase import Run2MCSampleBase, Run2UltraLegacyBase, Run2UltraLegacyStandardPDF
from utilities import cache, cacheaslist
from variationsample import MakeVariationSample, VariationSampleBase

class RepeatAsUltraLegacyBase(VariationSampleBase, Run2UltraLegacyBase):
  @property
  def variations(self):
    return super(RepeatAsUltraLegacyBase, self).variations+("UltraLegacy",)
  @property
  def extensionnumber(self):
    return 0
  @property
  def cmsswversion(self):
    if not os.path.exists(self.cvmfstarball):
      return "CMSSW_9_3_0"
    else:
      return "CMSSW_9_3_14"
  @property
  def scramarch(self):
    if not os.path.exists(self.cvmfstarball):
      return "slc6_amd64_gcc630"
    else:
      return "slc7_amd64_gcc630"

  def updateprepid(self):
    if not self.prepid or "wmLHEGEN" in self.prepid: return
    assert "wmLHEGS" in self.prepid
    if self.datasetname != self.fullinfo["dataset_name"]:
      raise ValueError("Dataset name is not consistent with the request on McM.  Please change it, if only temporarily.\n"+self.datasetname+"\n"+self.fullinfo["dataset_name"])

    oldprepid = self.prepid
    try:
      del self.prepid
      self.getprepid()
      assert self.prepid
    except:
      self.prepid = oldprepid
      print "failed", self.datasetname, self.extensionnumber
#      raise

@cache
def MakeRepeatAsUltraLegacySample(basecls, baseclsrun2, forsubclassing=False):
  for _ in basecls, Run2MCSampleBase:
    if not issubclass(baseclsrun2, _):
      raise TypeError("{} is not a subclass of {}".format(baseclsrun2, _))
  if issubclass(basecls, Run2MCSampleBase):
    raise TypeError("{} is a subclass of {}".format(basecls, Run2MCSampleBase))
  class RepeatAsUltraLegacy(RepeatAsUltraLegacyBase, MakeVariationSample(basecls)):
    @classmethod
    @cacheaslist
    def allsamples(cls):
      if forsubclassing and cls == RepeatAsUltraLegacy: return
      seen = []
      for sample in baseclsrun2.allsamples():
        seen.append(cls(*sample.initargs, **sample.initkwargs))
        yield seen[-1]

      for sample in baseclsrun2.allsamples():
        initargs, initkwargs = list(sample.initargs), dict(sample.initkwargs)
        assert initargs[0] in (2016, 2017, 2018), initargs[0]
        initargs[0] = 2016
        nextone = cls(*initargs, **initkwargs)
        if nextone in seen: continue
        seen.append(nextone)
        yield nextone

    mainsampletype = baseclsrun2

  RepeatAsUltraLegacy.__name__ = basecls.__name__+"UltraLegacy"
  return RepeatAsUltraLegacy

class RepeatHZZdandHZdZdAsUltraLegacy(MakeRepeatAsUltraLegacySample(MadGraphHZZdFromJake, MadGraphHZZdFromJakeRun2, forsubclassing=True), Run2UltraLegacyStandardPDF):
  @property
  def desiredPDForder(self): return "NNLO"
  @property
  def needPDFtobewellbehavedathighmass(self): return False

  def cvmfstarball_anyversion(self, version):
    if self.year == 2016 and self.VV == "ZZd":
      return type(self)(2018, *self.initargs[1:], **self.initkwargs).cvmfstarball_anyversion(version)
    return super(RepeatHZZdandHZdZdAsUltraLegacy, self).cvmfstarball_anyversion(version)

  @property
  def genproductionscommit(self):
    if self.year == 2016 and self.VV == "ZZd":
      return type(self)(2018, *self.initargs[1:], **self.initkwargs).genproductionscommit
    return super(RepeatHZZdandHZdZdAsUltraLegacy, self).genproductionscommit

  @property
  def madgraphcardscript(self):
    if self.year == 2016 and self.VV == "ZZd":
      return type(self)(2018, *self.initargs[1:], **self.initkwargs).madgraphcardscript
    return super(RepeatHZZdandHZdZdAsUltraLegacy, self).madgraphcardscript

  @property
  def madgraphcards(self):
    if self.year == 2016 and self.VV == "ZZd":
      return type(self)(2018, *self.initargs[1:], **self.initkwargs).madgraphcards
    return super(RepeatHZZdandHZdZdAsUltraLegacy, self).madgraphcards

  @property
  def datasetname(self):
    if self.VV == "ZZd":
      result = "HTo"+self.VV+"To4L_M125_MZd{}_eps1e-2_TuneCP5_13TeV_madgraph_pythia8".format(self.Zdmass)
      assert result == super(RepeatHZZdandHZdZdAsUltraLegacy, self).datasetname.replace("13TeV", "TuneCP5_13TeV")
      return result
    return super(RepeatHZZdandHZdZdAsUltraLegacy, self).datasetname

  @property
  def holdrequest(self):
    return True

class RepeatMCFMAsUltraLegacy(MakeRepeatAsUltraLegacySample(MCFMAnomCoupMCSample, MCFMAnomCoupMCSampleRun2, forsubclassing=True), Run2UltraLegacyStandardPDF):
  @property
  def desiredPDForder(self): return "LO"

  @property
  def tarballversion(self):
    v = self.mainsample.tarballversion

    identifierstr = " ".join(str(_) for _ in self.mainsample.identifiers)

    if self.year in (2016, 2017) or self.signalbkgbsi == "BKG":
      v+=1

      if identifierstr == "BSI 10 0PH ELMU": v+=1
      if identifierstr == "BSI 1 0M ELMU": v+=1
      if identifierstr == "BSI 10 0PM MUMU": v+=1
      if identifierstr == "BSI 10 0PHf05ph0 MUMU": v+=1
      if identifierstr == "BSI 10 0PL1f05ph0 TLTL": v+=1
      if identifierstr == "BSI 1 0M TLTL": v+=1
      if identifierstr == "BSI 10 0PH ELEL": v+=1
      if identifierstr == "BSI 10 0PHf05ph0 ELEL": v+=1
      if identifierstr == "BSI 10 0Mf05ph0 ELEL": v+=1

      if identifierstr == "BSI 10 0PM MUMU": v+=1
      if identifierstr == "BSI 10 0PHf05ph0 MUMU": v+=1
      if identifierstr == "BSI 1 0M TLTL": v+=1
      if identifierstr == "BSI 10 0PH ELEL": v+=1
      if identifierstr == "BSI 10 0PHf05ph0 ELEL": v+=1
      if identifierstr == "BSI 10 0Mf05ph0 ELEL": v+=1

      v+=1  #csmax patch

    if identifierstr == "BKG 1 0PM MUMU": v+=1

    if not (self.year == 2018 and self.signalbkgbsi == "BKG"):
      othersample = self.mainsampletype(2018, self.mainsample.signalbkgbsi, self.mainsample.width, self.mainsample.coupling, self.mainsample.finalstate)
      if self.mainsample.signalbkgbsi == "BKG":
        othersample = type(self)(*othersample.initargs, **othersample.initkwargs)
      assert v == othersample.tarballversion, (self, othersample, v, othersample.tarballversion)

    return v

  @property
  def genproductionscommit(self):
    return "a8ea4bc76df07ee2fa16bd9a67b72e7b648dec64"

  @property
  def cardsurl(self):
    with open("readInput.DAT") as f:
      for line in f:
        if "ncalls" in line and int(line.split()[0]) != 5000000:
          raise ValueError(line+"\nshould be 5000000")
    return super(RepeatMCFMAsUltraLegacy, self).cardsurl

  def cvmfstarball_anyversion(self, version):
    return self.mainsample.cvmfstarball_anyversion(version)  #avoid cmsswversion and scramarch infinite loop
