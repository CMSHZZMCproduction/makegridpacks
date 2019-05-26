class RepeatAsUltraLegacyBase(VariationSampleBase, Run2UltraLegacyBase): pass
  @property
  def variations(self):
    return super(RepeatAsUltraLegacyBase, self).variations+("UltraLegacy",)
  @property
  def extensionnumber(self):
    return 0

@cache
def MakeRepeatAsUltraLegacySample(basecls):
  class RepeatAsUltraLegacy(RepeatAsUltraLegacyBase, MakeVariationSample(basecls)): pass
  RepeatAsUltraLegacy.__name__ = basecls+"UltraLegacy"

class RepeatMCFMAsUltraLegacy(MakeRepeatAsUltraLegacySample(MCFMAnomCoupMCSample)):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    for sample in MCFMAnomCoupMCSample.allsamples():
      if sample.year == 2017:
        yield cls(*sample.initargs, **sample.initkwargs)
      if sample.year == 2018:
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

