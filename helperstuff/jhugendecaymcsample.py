import abc, re

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
