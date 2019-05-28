from gridpackbysomeoneelse import MadGraphHZZdFromJakeRun2
from madgraphmcsample import MadGraphMCSample
from mcfmanomalouscouplings import MCFMAnomCoupMCSample
from mcsamplebase import Run2UltraLegacyBase, Run2UltraLegacyStandardPDF
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
    #use the old slc because the gridpacks are typically the same as the main Run2 sample
    return "CMSSW_9_3_0"
  @property
  def scramarch(self):
    return "slc6_amd64_gcc630"

@cache
def MakeRepeatAsUltraLegacySample(basecls):
  class RepeatAsUltraLegacy(RepeatAsUltraLegacyBase, MakeVariationSample(basecls)): pass
  RepeatAsUltraLegacy.__name__ = basecls.__name__+"UltraLegacy"
  return RepeatAsUltraLegacy

class RepeatHZZdandHZdZdAsUltraLegacy(MakeRepeatAsUltraLegacySample(MadGraphHZZdFromJakeRun2), Run2UltraLegacyStandardPDF):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    for sample in cls.mainsampletype.allsamples():
      yield cls(*sample.initargs, **sample.initkwargs)

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

class RepeatMCFMAsUltraLegacy(MakeRepeatAsUltraLegacySample(MCFMAnomCoupMCSample)):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    return
    for sample in cls.mainsampletype.allsamples():
      yield cls(*sample.initargs, **sample.initkwargs)

  @property
  def tarballversion(self):
    v = self.mainsample.tarballversion+1

    identifierstr = " ".join(str(_) for _ in self.mainsample.identifiers)

    if year == 2017:
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

    if self.year == 2017:
      othersample = MCFMAnomCoupMCSample(2018, self.mainsample.signalbkgbsi, self.mainsample.width, self.mainsample.coupling, self.mainsample.finalstate)
      if self.mainsample.signalbkgbsi == "BKG":
        othersample = RepeatMCFMAsUltraLegacy(*othersample.initargs, **othersample.initkwargs)
      assert v == othersample.tarballversion, (v, othersample.tarballversion)

    return v

  @property
  def genproductionscommit(self):
    return "a8ea4bc76df07ee2fa16bd9a67b72e7b648dec64"

  def createtarball(self, *args, **kwargs):
    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.mainsample.cvmfstarball])
      with open("readInput.DAT") as f:
        for line in f:
          if "ncalls" in line:
            assert int(line.split()[0]) < 1000000, (self, self.mainsample.cvmfstarball, line)
    return super(RedoMCFMMoreNcalls, self).createtarball(*args, **kwargs)

  @property
  def cardsurl(self):
    with open("readInput.DAT") as f:
      for line in f:
        if "ncalls" in line and int(line.split()[0]) != 5000000:
          raise ValueError(line+"\nshould be 5000000")
    return super(RedoMCFMMoreNcalls, self).cardsurl
