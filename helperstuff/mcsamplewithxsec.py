import abc

import uncertainties

from utilities import cd, here

from mcsamplebase import MCSampleBase

class MCSampleWithXsec(MCSampleBase):
  @abc.abstractmethod
  def getxsec(self):
    "should return an uncertainties.ufloat"

  def makegridpack(self, *args, **kwargs):
    if not self.finished and not self.cvmfstarballexists:
      del self.xsec
    return super(MCSampleWithXsec, self).makegridpack(*args, **kwargs)

  @property
  def notes(self):
    result = super(MCSampleWithXsec, self).notes
    if result: result += "\n\n"
    return result+"cross section = {}".format(self.xsec)

  @property
  def xsecnominal(self):
    with cd(here):
      try:
        return self.value["xsec"]
      except KeyError:
        self.xsec = self.getxsec()
        return self.xsecnominal
  @xsecnominal.setter
  def xsecnominal(self, value):
    with cd(here), self.writingdict():
      self.value["xsec"] = value
    self.needsupdate = True
  @xsecnominal.deleter
  def xsecnominal(self):
    with cd(here), self.writingdict():
      del self.value["xsec"]

  @property
  def xsecerror(self):
    with cd(here):
      try:
        return self.value["xsecerror"]
      except KeyError:
        self.self.getxsec()
        return self.xsecerror
  @xsecerror.setter
  def xsecerror(self, value):
    with cd(here), self.writingdict():
      self.value["xsecerror"] = value
  @xsecerror.deleter
  def xsecerror(self):
    with cd(here), self.writingdict():
      del self.value["xsecerror"]

  @property
  def xsec(self):
    if self.xsecnominal is None or self.xsecerror is None: return None
    return uncertainties.ufloat(self.xsecnominal, self.xsecerror)
  @xsec.setter
  def xsec(self, value):
    nominal, error = uncertainties.nominal_value(value), uncertainties.std_dev(value)
    if error == 0 and nominal != 1: raise ValueError("Are you sure you want to set the xsec to {} with no error?".format(uncertainties.ufloat(nominal, error)))
    self.xsecnominal = nominal
    self.xsecerror = error
  @xsec.deleter
  def xsec(self):
    del self.xsecnominal, self.xsecerror