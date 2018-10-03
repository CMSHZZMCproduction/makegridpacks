import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from powhegmcsample import POWHEGMCSample

class POWHEGJHUGenMCSample(POWHEGMCSample):
  @abc.abstractproperty
  def decaycard(self): pass
  @property
  def hasfilter(self): return "filter" in self.decaycard.lower()
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

  @property
  def generators(self):
    assert re.match(r"v[0-9]+[.][0-9]+[.][0-9]+", self.JHUGenversion), self.JHUGenversion
    return super(POWHEGJHUGenMCSample, self).generators + ["JHUGen {}".format(self.JHUGenversion)]

  @abc.abstractproperty
  def JHUGenversion(self):
    pass
