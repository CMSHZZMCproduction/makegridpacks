import abc, re

from helperstuff.mcsamplebase import MCSampleBase

class JHUGenDecayMCSample(MCSampleBase):
  @abc.abstractproperty
  def decaycard(self): pass
  @property
  def hasfilter(self): return "filter" in self.decaycard.lower()

  @property
  def generators(self):
    assert re.match(r"v[0-9]+[.][0-9]+[.][0-9]+", self.JHUGenversion), self.JHUGenversion
    return super(JHUGenDecayMCSample, self).generators + ["JHUGen {}".format(self.JHUGenversion)]

  @abc.abstractproperty
  def JHUGenversion(self):
    pass
