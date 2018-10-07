import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from powhegmcsample import POWHEGMCSample
from jhugendecaymcsample import JHUGenDecayMCSample

class POWHEGJHUGenMCSample(POWHEGMCSample, JHUGenDecayMCSample):
  @property
  def makegridpackcommand(self):
    return super(POWHEGJHUGenMCSample, self).makegridpackcommand + ["-g", self.decaycard]
  @property
  def foldernameforrunpwg(self):
    return super(POWHEGJHUGenMCSample, self).foldernameforrunpwg+"_"+self.decaymode

  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])
    result = super(POWHEGJHUGenMCSample, self).cardsurl + "\n# " + JHUGencard

    with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
      JHUGengitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      if glob.glob("core.*"):
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open("JHUGen.input") as f:
          JHUGencard = f.read()
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}".format(self))

    if JHUGencard != JHUGengitcard:
      raise ValueError("JHUGencard != JHUGengitcard\n{}\n{}\n{}".format(self, JHUGencard, JHUGengitcard))

    return result
