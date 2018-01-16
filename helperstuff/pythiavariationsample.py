import contextlib, csv, os, re, subprocess, urllib

from mcsamplebase import MCSampleBase
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

class PythiaVariationSample(MCSampleBase):
  def __init__(self, mainsample, variation):
    """
    mainsample - nominal sample that this is similar to
    variation - ScaleUp, ScaleDown, TuneUp, TuneDown
    """
    self.mainsample = mainsample
    self.variation = variation
    if self.matchefficiency is None:
      self.matchefficiency = self.mainsample.matchefficiency
    if self.matchefficiencyerror is None:
      self.matchefficiencyerror = self.mainsample.matchefficiencyerror
    if self.timeperevent is None:
      self.timeperevent = self.mainsample.timeperevent
    if self.sizeperevent is None:
      self.sizeperevent = self.mainsample.sizeperevent
    if (self.matchefficiency, self.matchefficiencyerror) != (self.mainsample.matchefficiency, self.mainsample.matchefficiencyerror):
      raise ValueError("Match efficiency doesn't match!\n{}, {}\n{} +/- {}, {} +/- {}".format(
        self, self.mainsample, self.matchefficiency, self.matchefficiencyerror, self.mainsample.matchefficiency, self.mainsample.matchefficiencyerror
      ))
  @property
  def identifiers(self):
    return self.mainsample.identifiers + (self.variation,)
  @property
  def tarballversion(self):
    return self.mainsample.tarballversion
  @property
  def cvmfstarball(self):
    return self.mainsample.cvmfstarball
  @property
  def tmptarball(self):
    return self.mainsample.tmptarball
  @property
  def makegridpackcommand(self):
    return self.mainsample.makegridpackcommand
  @property
  def makinggridpacksubmitsjob(self):
    return self.mainsample.makinggridpacksubmitsjob
  @property
  def hasfilter(self):
    return self.mainsample.hasfilter
  @property
  def datasetname(self):
    result = self.mainsample.datasetname
    result = result.replace("13TeV", "13TeV_"+self.variation.lower())
    assert self.variation.lower() in result
    return result
  @property
  def nevents(self):
    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample):
      if self.mainsample.productionmode in ("ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH") and self.mainsample.mass == 125 and self.mainsample.decaymode == "4l":
        return 500000
    if instance(self.mainsample, MINLOMCSample):
      if self.mainsample.mass in (125, 300):
        return 1000000
    raise ValueError("No nevents for {}".format(self))
  @property
  def generators(self):
    return self.mainsample.generators
  @property
  def cardsurl(self):
    return self.mainsample.cardsurl
  @property
  def defaulttimeperevent(self):
    if self.mainsample.timeperevent is not None:
      return self.mainsample.timeperevent
    return self.mainsample.defaulttimeperevent
  @property
  def tags(self):
    return "HZZ", "Fall17P2A"
  @property
  def fragmentname(self):
    result = self.mainsample.fragmentname
    if self.variation == "TuneUp":
      result = result.replace("CP5", "CP5Up")
    elif self.variation == "ScaleUp":
      result = result.replace("CP5", "CP5_ScaleUp")
    elif self.variation == "TuneDown":
      result = result.replace("CP5", "CP5Down")
    elif self.variation == "ScaleDown":
      result = result.replace("CP5", "CP5_ScaleDown")
    else:
      assert False
    assert result != self.mainsample.fragmentname
    return result
  @property
  def genproductionscommit(self):
    return "9a5a9d535223760d4df4e66001f701bb1f1426e8"
  @property
  def makegridpackscriptstolink(self):
    return self.mainsample.makegridpackscriptstolink
  @property
  def keepoutput(self):
    return self.mainsample.keepoutput
  @property
  def responsible(self):
    return "hroskes"

  @classmethod
  def nominalsamples(cls):
    for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
      yield POWHEGJHUGenMassScanMCSample(productionmode, "4l", 125)
    return
    for mass in 125, 300:
      yield MINLOMCSample("4l", mass)

  @classmethod
  def allsamples(cls):
    for nominal in cls.nominalsamples():
      for systematic in "TuneUp", "TuneDown", "ScaleUp", "ScaleDown":
        yield cls(nominal, systematic)
