import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from powhegmcsample import POWHEGMCSample

class POWHEGJHUGenMCSample(POWHEGMCSample):
  @abc.abstractproperty
  def decaycard(self): pass
  @property
  def hasfilter(self): return "filter" in self.decaycard.lower()
  def makegridpackcommand(self, *args, **kwargs):
    return super(POWHEGJHUGenMCSample, self).makegridpackcommand(*args, **kwargs) + ["-g", self.decaycard]

  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])
    result = super(POWHEGJHUGenMCSample, self).cardsurl + "\n# " + JHUGencard

    with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
      JHUGengitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvzf", self.cvmfstarball])
      if glob.glob("core.*") and self.cvmfstarball != "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/HJJ_M125_13TeV/HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_13TeV_M125.tgz":
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open("JHUGen.input") as f:
          JHUGencard = f.read()
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}".format(self))

    if JHUGencard != JHUGengitcard:
      raise ValueError("JHUGencard != JHUGengitcard\n{}\n{}\n{}".format(self, JHUGencard, JHUGengitcard))

    return result

  @property
  def generators(self):
    return super(POWHEGJHUGenMCSample, self).generators + ["JHUGen v7.0.11"]
