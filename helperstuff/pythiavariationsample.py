import contextlib, csv, os, re, subprocess, urllib

from mcsamplebase import MCSampleBase
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
from minlomcsample import MINLOMCSample

class PythiaVariationSample(MCSampleBase):
  def __init__(self, mainsample, variation):
    """
    mainsample - nominal sample that this is similar to
    variation - ScaleExtension, TuneUp, TuneDown
    """
    self.mainsample = mainsample
    self.variation = variation
    if self.matchefficiency is None:
      self.matchefficiency = self.mainsample.matchefficiency
    if self.matchefficiencyerror is None:
      self.matchefficiencyerror = self.mainsample.matchefficiencyerror
    if self.timeperevent is None and self.mainsample.timeperevent is not None:
      self.timeperevent = self.mainsample.timeperevent
    if self.sizeperevent is None and self.mainsample.sizeperevent is not None:
      self.sizeperevent = self.mainsample.sizeperevent
    if (self.matchefficiency, self.matchefficiencyerror) != (self.mainsample.matchefficiency, self.mainsample.matchefficiencyerror) and self.mainsample.matchefficiency is not None is not self.mainsample.matchefficiencyerror:
      raise ValueError("Match efficiency doesn't match!\n{}, {}\n{} +/- {}, {} +/- {}".format(
        self, self.mainsample, self.matchefficiency, self.matchefficiencyerror, self.mainsample.matchefficiency, self.mainsample.matchefficiencyerror
      ))
  @property
  def identifiers(self):
    return self.mainsample.identifiers + (self.variation,)
  @property
  def tarballversion(self):
    return self.mainsample.tarballversion
  def cvmfstarball_anyversion(self, version):
    return self.mainsample.cvmfstarball_anyversion(version)
  @property
  def tmptarball(self):
    return self.mainsample.tmptarball
  def createtarball(self):
    return "this is a variation sample, the gridpack is the same as for the main sample"
  def findmatchefficiency(self):
    return "this is a variation sample, the filter efficiency is the same as for the main sample"
  @property
  def workdir(self):
    return self.mainsample.workdir.rstrip("/")+"_"+self.variation
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
  def xsec(self):
    return self.mainsample.xsec
  @property
  def datasetname(self):
    result = self.mainsample.datasetname
    if self.variation != "ScaleExtension":
      result = result.replace("13TeV", "13TeV_"+self.variation.lower())
      assert self.variation.lower() in result
    return result
  @property
  def nevents(self):
    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample):
      if self.mainsample.productionmode in ("ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH") and self.mainsample.mass == 125 and self.mainsample.decaymode == "4l":
        if self.variation == "ScaleExtension":
          return 1000000
        else:
          return 500000
    if isinstance(self.mainsample, MINLOMCSample):
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
    elif self.variation == "TuneDown":
      result = result.replace("CP5", "CP5Down")
    elif self.variation == "ScaleExtension":
      pass
    else:
      assert False

    if self.variation != "ScaleExtension":
      assert result != self.mainsample.fragmentname
    return result
  @property
  def genproductionscommit(self):
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"
  @property
  def makegridpackscriptstolink(self):
    return self.mainsample.makegridpackscriptstolink
  @property
  def doublevalidationtime(self):
    return self.mainsample.doublevalidationtime
  @property
  def neventsfortest(self): return self.mainsample.neventsfortest
  @property
  def creategridpackqueue(self): return self.mainsample.creategridpackqueue
  @property
  def timepereventqueue(self): return self.mainsample.timepereventqueue
  @property
  def filterefficiencyqueue(self): return self.mainsample.filterefficiencyqueue
  @property
  def extensionnumber(self):
    result = super(PythiaVariationSample, self).extensionnumber
    if self.variation == "ScaleExtension": result += 1
    return result
  @property
  def responsible(self):
    if isinstance(self.mainsample, MINLOMCSample): return "wahung"
    return "hroskes"
  @property
  def dovalidation(self):
    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample) and self.mainsample.productionmode == "ZH": return False
    return self.mainsample.dovalidation

  @classmethod
  def nominalsamples(cls):
    for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
      yield POWHEGJHUGenMassScanMCSample(productionmode, "4l", 125)
    for sample in MINLOMCSample.allsamples():
      if sample.energy == 13:
        yield sample

  @classmethod
  def allsamples(cls):
    for nominal in cls.nominalsamples():
      for systematic in "TuneUp", "TuneDown", "ScaleExtension":
        if isinstance(nominal, MINLOMCSample) and systematic == "ScaleExtension": continue
        yield cls(nominal, systematic)
