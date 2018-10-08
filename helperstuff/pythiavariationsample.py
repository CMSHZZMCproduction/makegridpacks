from collections import namedtuple
import contextlib, csv, os, re, subprocess, urllib

from mcsamplebase import MCSampleBase
from minlomcsample import MINLOMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
from utilities import here

class VariationSample(MCSampleBase):
  def __init__(self, mainsample, variation):
    """
    mainsample - nominal sample that this is similar to
    variation - ScaleExtension, TuneUp, TuneDown
    """
    self.mainsample = mainsample
    self.variation = variation
    super(VariationSample, self).__init__(year=mainsample.year)
    if self.matchefficiency is None:
      self.matchefficiency = self.mainsample.matchefficiency
    if (self.matchefficiencynominal, self.matchefficiencyerror) != (self.mainsample.matchefficiencynominal, self.mainsample.matchefficiencyerror) and self.mainsample.matchefficiencynominal is not None is not self.mainsample.matchefficiencyerror:
      raise ValueError("Match efficiency doesn't match!\n{}, {}\n{}, {}".format(
        self, self.mainsample, self.matchefficiency, self.mainsample.matchefficiency
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
  def patchtarball(self):
    samples = (
      [self.mainsample] +
      [s for s in self.allsamples() if s.mainsample == self.mainsample]
    )

    needspatchparameters = {
      _.needspatch for _ in samples if _.needspatch
    }
    assert len(needspatchparameters) == 1
    self.mainsample.needspatch = self.needspatch
    result = self.mainsample.patchtarball()
    if result == "tarball is patched and the new version is in this directory to be copied to eos":
      for _ in samples: _.needspatch = False
      return result
    elif result == "job to patch the tarball is already running" or result is None:
      return result
    else:
      raise ValueError("Unknown result from patchtarball:\n{}".format(result))
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
  def campaign(self):
    return self.mainsample.campaign
  @property
  def productiongenerators(self):
    return self.mainsample.productiongenerators
  @property
  def decaygenerators(self):
    return self.mainsample.decaygenerators
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
    return self.mainsample.tags
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
  def dovalidation(self): return self.mainsample.dovalidation
  @property
  def fragmentname(self): return self.mainsample.fragmentname

class ExtensionSample(VariationSample):
  def __init__(self, *args, **kwargs):
    super(ExtensionSample, self).__init__(*args, **kwargs)
    if self.timeperevent is None and self.mainsample.timeperevent is not None and not self.resettimeperevent:
      self.timeperevent = self.mainsample.timeperevent * self.mainsample.nthreads / self.nthreads
    if self.sizeperevent is None and self.mainsample.sizeperevent is not None:
      self.sizeperevent = self.mainsample.sizeperevent
  @property
  def datasetname(self): return self.mainsample.datasetname
  @property
  def fragmentname(self): return self.mainsample.fragmentname
  @property
  def genproductionscommit(self): return self.mainsample.genproductionscommit
  @property
  def extensionnumber(self): return self.mainsample.extensionnumber+1

class RedoSample(ExtensionSample):
  @property
  def nevents(self): return self.mainsample.nevents

class RunIIFall17DRPremix_nonsubmitted(RedoSample):
  @classmethod
  def allsamples(cls):
    cls.__inallsamples = True
    requests = []
    Request = namedtuple("Request", "dataset prepid url")
    with open(os.path.join(here, "data", "ListRunIIFall17DRPremix_nonsubmitted.txt")) as f:
      next(f); next(f); next(f)  #cookie and header
      for line in f:
        requests.append(Request(*line.split()))

    from . import allsamples
    for s in allsamples(onlymysamples=False, clsfilter=lambda cls2: cls2 != cls, __docheck=False):
      if any(_.prepid == s.prepid for _ in requests):
        yield cls(mainsample=s, variation="RunIIFall17DRPremix_nonsubmitted")

  @property
  def doublevalidationtime(self):
    if self.prepid in ("HIG-RunIIFall17wmLHEGS-03116", "HIG-RunIIFall17wmLHEGS-03155"): return True
    return super(RunIIFall17DRPremix_nonsubmitted, self).doublevalidationtime

  @property
  def responsible(self):
    return "hroskes"

class PythiaVariationSample(VariationSample):
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
    if self.prepid == "HIG-RunIIFall17wmLHEGS-00509": return False
    return super(PythiaVariationSample, self).dovalidation

  @classmethod
  def nominalsamples(cls):
    for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
      yield POWHEGJHUGenMassScanMCSample(2017, productionmode, "4l", 125)
    for sample in MINLOMCSample.allsamples():
      if sample.energy == 13:
        yield sample

  @classmethod
  def allsamples(cls):
    for nominal in cls.nominalsamples():
      for systematic in "TuneUp", "TuneDown", "ScaleExtension":
        if isinstance(nominal, MINLOMCSample) and systematic == "ScaleExtension": continue
        yield cls(nominal, systematic)
