import abc, contextlib, os, re

from utilities import urlopen, wget
from filtermcsample import JHUGenFilter

class JHUGenDecayMCSample(JHUGenFilter):
  @abc.abstractproperty
  def decaycard(self): pass
  @property
  def hasJHUGenfilter(self): return "filter" in self.decaycard.lower()
  @abc.abstractproperty
  def hasnonJHUGenfilter(self):
    return super(JHUGenDecayMCSample, self).hasfilter
  @property
  def hasfilter(self):
    result = self.hasJHUGenfilter or self.hasnonJHUGenfilter
    if super(JHUGenDecayMCSample, self).hasfilter: assert result, self
    return result

  @property
  def decaygenerators(self):
    assert re.match(r"v[0-9]+[.][0-9]+[.][0-9]+", self.JHUGenversion), self.JHUGenversion
    return super(JHUGenDecayMCSample, self).decaygenerators + ["JHUGen {}".format(self.JHUGenversion)]

  @abc.abstractproperty
  def JHUGenversion(self):
    pass

  @abc.abstractproperty
  def JHUGencardlocationintarball(self):
    pass

  @property
  def genproductionscommitforfragment(self): return self.genproductionscommit

  @property
  def cardsurl(self):
    commit = self.genproductionscommitfordecay
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])
    result = JHUGencard
    moreresult = super(JHUGenDecayMCSample, self).cardsurl
    if moreresult: result += "\n# " + moreresult

    with contextlib.closing(urlopen(JHUGencard)) as f:
      JHUGengitcard = f.read()

    try:
      with open(self.JHUGencardlocationintarball) as f:
        JHUGencard = f.read()
    except IOError:
      raise ValueError("no {0.JHUGencardlocationintarball} in the tarball\n{0}".format(self))

    if JHUGencard != JHUGengitcard:
      raise ValueError("JHUGencard != JHUGengitcard\n{}\n{}\n{}".format(self, JHUGencard, JHUGengitcard))

    return result
