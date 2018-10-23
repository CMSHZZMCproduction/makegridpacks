from collections import namedtuple
import abc, contextlib, csv, os, re, subprocess, urllib

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
    if self.filterefficiency is None:
      self.filterefficiency = self.mainsample.filterefficiency
    if (self.filterefficiencynominal, self.filterefficiencyerror) != (self.mainsample.filterefficiencynominal, self.mainsample.filterefficiencyerror) and self.mainsample.filterefficiencynominal is not None is not self.mainsample.filterefficiencyerror:
      raise ValueError("Match efficiency doesn't match!\n{}, {}\n{}, {}".format(
        self, self.mainsample, self.filterefficiency, self.mainsample.filterefficiency
      ))
  @property
  def mainmainsample(self):
    if hasattr(self.mainsample, "mainsample"):
      return self.mainsample.mainsample
    return self.mainsample
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
  def patchtarball(self):
    samples = (
      [self.mainmainsample] +
      [s for s in self.allsamples() if s.mainmainsample == self.mainmainsample]
    )

    needspatchparameters = {
      _.needspatch for _ in samples if _.needspatch
    }
    assert len(needspatchparameters) == 1
    self.mainmainsample.needspatch = self.needspatch
    result = self.mainmainsample.patchtarball()
    if result == "tarball is patched and the new version is in this directory to be copied to eos":
      for _ in samples: _.needspatch = False
      return result
    elif result == "job to patch the tarball is already running" or result is None:
      return result
    else:
      raise ValueError("Unknown result from patchtarball:\n{}".format(result))
  @property
  def makinggridpacksubmitsjob(self):
    return self.mainsample.makinggridpacksubmitsjob
  @property
  def inthemiddleofmultistepgridpackcreation(self):
    return self.mainsample.inthemiddleofmultistepgridpackcreation
  @property
  def gridpackjobsrunning(self):
    return self.mainsample.gridpackjobsrunning
  def findfilterefficiency(self):
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
  def processmakegridpackstdout(self, stdout):
    return self.mainsample.processmakegridpackstdout(stdout)
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
  def handle_request_fragment_check_warning(self, line):
    if line.strip() == "* [WARNING] Large time/event - please check":
      return super(VariationSample, self).handle_request_fragment_check_warning(line)
    return self.mainsample.handle_request_fragment_check_warning(line)
  def handle_request_fragment_check_caution(self, line):
    return self.mainsample.handle_request_fragment_check_caution(line)
  @property
  def maxallowedtimeperevent(self):
    return self.mainsample.maxallowedtimeperevent

class ExtensionSampleBase(VariationSample):
  def __init__(self, *args, **kwargs):
    super(ExtensionSampleBase, self).__init__(*args, **kwargs)
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

class ExtensionSample(ExtensionSampleBase):
  """
  for extensions that are identical to the main sample
  except in the number of events
  """
  @property
  def nevents(self):
    from qqZZmcsample import QQZZMCSample
    if isinstance(self.mainmainsample, QQZZMCSample) and self.mainmainsample.cut is None:
      if self.year == 2018:
        if self.mainmainsample.finalstate == "4l": return 100000000
        if self.mainmainsample.finalstate == "2l2nu": return 50000000
    assert False, self

  @classmethod
  def samplestoextend(cls):
    from qqZZmcsample import QQZZMCSample
    yield RedoSample(QQZZMCSample(2018, "4l"))
    yield RedoSample(QQZZMCSample(2018, "2l2nu"))

  @classmethod
  def allsamples(cls):
    for _ in cls.samplestoextend():
      yield cls(_, "ext")

  @property
  def responsible(self):
    return self.mainsample.responsible

  def handle_request_fragment_check_warning(self, line):
    from qqZZmcsample import QQZZMCSample
    if self.mainmainsample == QQZZMCSample(2018, "4l"):
      if line.strip() == "* [WARNING] Is 100000000 events what you really wanted - please check!":
        #yes it is
        return "ok"
    return super(ExtensionSample, self).process_request_fragment_check_warning(line)

class RedoSampleBase(ExtensionSampleBase):
  def __init__(self, mainsample, reason=None):
    self.__reason = reason
    return super(RedoSampleBase, self).__init__(mainsample=mainsample, variation=self.variationname)

  @abc.abstractproperty
  def variationname(self): "can be a class variable"

  @property
  def nevents(self): return self.mainsample.nevents

  @property
  def notes(self):
    result = "Redo of " + self.mainsample.prepid
    if self.__reason is not None: result += " "+self.__reason
    return result

class RedoSample(RedoSampleBase):
  @classmethod
  def allsamples(cls):
    from qqZZmcsample import QQZZMCSample
    yield cls(QQZZMCSample(2018, "4l"), reason="because it was prematurely force completed")
    yield cls(QQZZMCSample(2018, "2l2nu"), reason="because it was prematurely force completed")

  variationname = "Redo"

  @property
  def responsible(self):
    return self.mainsample.responsible

class RunIIFall17DRPremix_nonsubmitted(RedoSampleBase):
  variationname = "RunIIFall17DRPremix_nonsubmitted"

  @classmethod
  def allsamples(cls):
    cls.__inallsamples = True
    requests = []
    Request = namedtuple("Request", "dataset prepid url")
    with open(os.path.join(here, "data", "ListRunIIFall17DRPremix_nonsubmitted.txt")) as f:
      next(f); next(f); next(f)  #cookie and header
      for line in f:
        requests.append(Request(*line.split()))
    with open(os.path.join(here, "data", "ListRunIIFall17DRPremix_nonsubmitted_2.txt")) as f:
      for line in f:
        requests.append(Request(*line.split()))

    from . import allsamples
    from jhugenjhugenanomalouscouplings import JHUGenJHUGenAnomCoupMCSample
    from powhegjhugenanomalouscouplings import POWHEGJHUGenAnomCoupMCSample
    from jhugenjhugenmassscanmcsample import JHUGenJHUGenMassScanMCSample
    from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
    from mcfmanomalouscouplings import MCFMAnomCoupMCSample
    from pythiavariationsample import PythiaVariationSample
    for s in allsamples(onlymysamples=False, clsfilter=lambda cls2: cls2 in (JHUGenJHUGenAnomCoupMCSample, POWHEGJHUGenAnomCoupMCSample, JHUGenJHUGenMassScanMCSample, POWHEGJHUGenMassScanMCSample, MCFMAnomCoupMCSample, PythiaVariationSample), __docheck=False, includefinished=True, filter=lambda x: x.year == 2017):
      if any(_.prepid == s.prepid for _ in requests):
        yield cls(mainsample=s, reason="\n\nRunIIFall17DRPremix_nonsubmitted")

  @property
  def doublevalidationtime(self):
    if self.prepid in ("HIG-RunIIFall17wmLHEGS-03116", "HIG-RunIIFall17wmLHEGS-03155"): return True
    return super(RunIIFall17DRPremix_nonsubmitted, self).doublevalidationtime

  @property
  def responsible(self):
    return "hroskes"

  @property
  def extensionnumber(self):
    from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
    result = super(RunIIFall17DRPremix_nonsubmitted, self).extensionnumber
    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample) and self.mainsample.mass == 125:
      result += 1
      if self.mainsample.productionmode == "ggH":
        result += 1
    return result

  @property
  def genproductionscommitforfragment(self):
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def tarballversion(self):
    v = super(RunIIFall17DRPremix_nonsubmitted, self).tarballversion
    from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample

    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample) and self.mainsample.productionmode == "ZH" and self.mainsample.decaymode == "4l" and self.mainsample.mass not in (125, 165, 170): v+=1  #removing some pdfs
    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample) and self.mainsample.productionmode == "ZH" and self.mainsample.decaymode == "4l" and self.mainsample.mass in (120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 175, 180, 190, 200, 210, 250, 270, 300, 400, 450, 550, 600, 700, 1000, 2000, 2500, 3000): v+=1 #try multicore
    if isinstance(self.mainsample, POWHEGJHUGenMassScanMCSample) and self.mainsample.productionmode == "ttH" and self.mainsample.decaymode == "4l" and self.mainsample.mass == 140: v+=1  #tweak seed to avoid fluctuation in filter efficiency
    return v

class PythiaVariationSample(VariationSample):
  @property
  def datasetname(self):
    result = self.mainsample.datasetname
    if self.variation != "ScaleExtension":
      result = result.replace("13TeV", "13TeV_"+self.variation.lower())
      assert self.variation.lower() in result
    return result
  @property
  def tarballversion(self):
    result = super(PythiaVariationSample, self).tarballversion
    if self.prepid == "HIG-RunIIFall17wmLHEGS-00509": result += 1
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
    return "hroskes"

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
