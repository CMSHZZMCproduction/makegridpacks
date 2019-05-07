from collections import namedtuple
import abc, contextlib, csv, itertools, json, os, re, subprocess

from mcsamplebase import MCSampleBase
from utilities import abstractclassmethod, cache, cacheaslist, cdtemp, fullinfo, here, recursivesubclasses

from gridpackbysomeoneelse import MadGraphHJJFromThomasPlusJHUGenRun2
from jhugenjhugenanomalouscouplings import JHUGenJHUGenAnomCoupMCSampleRun2
from jhugenjhugenmassscanmcsample import JHUGenJHUGenMassScanMCSampleRun2
from jhugenmcsample import JHUGenMCSample
from mcfmanomalouscouplings import MCFMAnomCoupMCSampleRun2
from minlomcsample import MINLOMCSampleRun2
from powhegjhugenanomalouscouplings import POWHEGJHUGenAnomCoupMCSampleRun2
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSampleRun2
from qqZZmcsample import QQZZMCSampleRun2

@cache
def NthOrderVariationSampleBase(n, *flags):
  flags = list(frozenset(flags))
  if sorted(flags) != flags: return NthOrderVariationSampleBase(n, *sorted(flags))

  if "JHUGen" in flags:
    flags.remove("JHUGen")
    base = NthOrderVariationSampleBase(n, *flags)
    class theclass(base, JHUGenMCSample):
      @property
      def shortname(self):
        return self.mainmainsample.shortname
    theclass.__name__ = base.__name__+"JHUGen"
    return theclass

  if flags: raise ValueError("Unknown flags: "+", ".join(flags))

  if n == 0: return MCSampleBase
  class VariationSampleBase(NthOrderVariationSampleBase(n-1)):
    @abc.abstractproperty
    def mainsample(self): pass
    @property
    def mainmainsample(self):
      if hasattr(self.mainsample, "mainsample"):
        return self.mainsample.mainsample
      return self.mainsample
    @property
    def previousprepids(self):
      if isinstance(self.mainsample, VariationSampleBase):
        return self.mainsample.previousprepids + (self.prepid,)
      return self.mainsample.prepid, self.prepid
    @classmethod
    def allsamples(cls): return ()
    mainsampletype = None
  if n>1: VariationSampleBase.__name__ += str(n)
  return VariationSampleBase

VariationSampleBase = NthOrderVariationSampleBase(1)

class JHUGenVariationSampleBase(VariationSampleBase, JHUGenMCSample):
  @property
  def shortname(self):
    return self.mainmainsample.shortname

@cache
def MakeVariationSample(basecls):
  for i in itertools.count(0):
    if not issubclass(basecls, NthOrderVariationSampleBase(i)): break
  assert i > 0

  flags = []
  if issubclass(basecls, JHUGenMCSample):
    flags.append("JHUGen")

  class VariationSample(NthOrderVariationSampleBase(i, *flags), basecls):
    def __init__(self, *args, **kwargs):
      super(VariationSample, self).__init__(*args, **kwargs)
      if self.filterefficiency is None and self.mainsample.filterefficiency is not None:
        self.filterefficiency = self.mainsample.filterefficiency
      if (self.filterefficiencynominal, self.filterefficiencyerror) != (self.mainsample.filterefficiencynominal, self.mainsample.filterefficiencyerror) and self.mainsample.filterefficiencynominal is not None is not self.mainsample.filterefficiencyerror:
        raise ValueError("Filter efficiency doesn't match!\n{}, {}\n{}, {}".format(
          self, self.mainsample, self.filterefficiency, self.mainsample.filterefficiency
        ))
    mainsampletype = basecls
    @property
    def mainsample(self):
      return basecls(*self.mainsampleinitargs, **self.mainsampleinitkwargs)
    #initargs is inherited from basecls and can be overridden
    @property
    def mainsampleinitargs(self):
      return super(VariationSample, self).initargs
    @property
    def mainsampleinitkwargs(self):
      return super(VariationSample, self).initkwargs
    @property
    def identifiers(self):
      if isinstance(self, NthOrderVariationSampleBase(i+1)):
        return super(VariationSample, self).identifiers

      if len(self.variations) != i:
        from MROgraph import MROgraph
        MROgraph(type(self))
        raise ValueError("Wrong number of variations\n{}\n{}".format(self.variations, i))
      return super(VariationSample, self).identifiers + self.variations

    def findfilterefficiency(self):
      return "this is a variation sample, the filter efficiency is the same as for the main sample"
    @property
    def defaulttimeperevent(self):
      if self.mainsample.timeperevent is not None:
        return self.mainsample.timeperevent
      return super(VariationSample, self).defaulttimeperevent

    @abc.abstractproperty
    def variations(self):
      if i == 1: return ()
      return super(VariationSample, self).variations

  VariationSample.__name__ = "VariationOf"+basecls.__name__

  return VariationSample

@cache
def NthOrderExtensionSampleGlobalBase(n):
  if n == 0: return MCSampleBase
  class ExtensionSampleGlobalBase(NthOrderVariationSampleBase(n), NthOrderExtensionSampleGlobalBase(n-1)): pass
  if n>1: ExtensionSampleGlobalBase.__name__ += str(n)
  return ExtensionSampleGlobalBase

ExtensionSampleGlobalBase = NthOrderExtensionSampleGlobalBase(1)

@cache
def MakeExtensionSampleBase(basecls):
  for i in itertools.count(0):
    if not issubclass(basecls, NthOrderExtensionSampleGlobalBase(i)): break
  assert i > 0
  class ExtensionSampleBase(NthOrderExtensionSampleGlobalBase(i), MakeVariationSample(basecls)):
    @property
    def extensionnumber(self): return super(ExtensionSampleBase, self).extensionnumber+1
    @abc.abstractproperty
    def nevents(self): return super(ExtensionSampleBase, self).nevents
  ExtensionSampleBase.__name__ = "ExtensionBaseOf"+basecls.__name__
  return ExtensionSampleBase

class ExtensionSampleBase(ExtensionSampleGlobalBase): pass

@cache
def MakeExtensionSample(basecls):
  class ExtensionSample(ExtensionSampleBase, MakeExtensionSampleBase(basecls)):
    """
    for extensions that are identical to the main sample
    except in the number of events
    """
    @property
    def variations(self): return super(ExtensionSample, self).variations + ("ext",)

    @property
    def notes(self):
      if isinstance(self.mainsample, RunIIFall17DRPremix_nonsubmittedBase): return self.mainmainsample.notes
      return super(ExtensionSample, self).notes
  ExtensionSample.__name__ = "ExtensionOf"+basecls.__name__
  return ExtensionSample

class ExtensionOfQQZZSampleBase(MCSampleBase):
  def handle_request_fragment_check_warning(self, line):
    if self.finalstate == "4l":
      if line.strip() == "* [WARNING] Is 100000000 events what you really wanted - please check!":
        #yes it is
        return "ok"
    return super(ExtensionOfQQZZSampleBase, self).handle_request_fragment_check_warning(line)

  @property
  def nevents(self):
    if self.cut is None:
      if self.year == 2018 or self.year == 2017:
        if self.finalstate == "4l": return 100000000
        if self.finalstate == "2l2nu": return 50000000

    assert False, self

class ExtensionOfQQZZSampleRun2(ExtensionOfQQZZSampleBase, MakeExtensionSample(QQZZMCSampleRun2)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, "4l")

class ExtensionOfJHUGenJHUGenAnomalousCouplingsRun2(MakeExtensionSample(JHUGenJHUGenAnomCoupMCSampleRun2)):
  @classmethod
  def allsamples(cls):
    yield cls(2018, "HJJ", "4l", 125, "SM")
    yield cls(2018, "HJJ", "4l", 125, "a3")
    yield cls(2018, "HJJ", "4l", 125, "a3mix")
  @property
  def nevents(self):
    if productionmode == "HJJ":
      return 1500000 - 250000

class ExtensionOfMadGraphHJJFromThomasPlusJHUGenRun2(MakeExtensionSample(MadGraphHJJFromThomasPlusJHUGenRun2)):
  @classmethod
  def allsamples(cls):
    for year in 2016, 2017, 2018:
      for coupling in "SM", "a3", "a3mix":
        yield cls(year, coupling, "H012J")

  @property
  def nevents(self): return 3000000 - 500000

class RedoSampleGlobalBase(ExtensionSampleGlobalBase):
  @property
  def reason(self): return None
  @property
  def nevents(self):
    #in an earlier base class this is abstract.  not anymore.
    return super(RedoSampleGlobalBase, self).nevents
  @property
  def notes(self):
    result = "Redo of " + self.mainsample.prepid
    if self.reason is not None: result += " "+self.reason
    supernotes = super(RedoSampleGlobalBase, self).notes
    return "\n\n".join(_ for _ in (result, supernotes) if _)

@cache
def MakeRedoSampleBase(basecls):
  class RedoSampleBase(RedoSampleGlobalBase, MakeExtensionSampleBase(basecls)): pass
  RedoSampleBase.__name__ = "RedoBaseOf"+basecls.__name__
  return RedoSampleBase

class RedoSampleBase(RedoSampleGlobalBase):
  @property
  def variations(self): return super(RedoSampleBase, self).variations + ("Redo",)
  def __init__(self, *args, **kwargs):
    self.__reason = kwargs.pop("reason", None)
    super(RedoSampleBase, self).__init__(*args, **kwargs)
  @property
  def reason(self):
    return self.__reason


@cache
def MakeRedoSample(basecls):
  class RedoSample(RedoSampleBase, MakeRedoSampleBase(basecls)): pass
  RedoSample.__name__ = "Redo"+basecls.__name__
  return RedoSample

class RedoPOWHEGJHUGenMassScanRun2(MakeRedoSample(POWHEGJHUGenMassScanMCSampleRun2)):
  @classmethod
  def allsamples(cls):
    for productionmode in "VBF", "ttH":
      yield cls(2016, productionmode, "4l", 125, reason="to check compatibility between MiniAODv2 and v3\n\nhttps://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/5977/1.html")

  @property
  def validationtimemultiplier(self):
    result = super(RedoPOWHEGJHUGenMassScan, self).validationtimemultiplier
    if "HIG-RunIISummer15wmLHEGS-01906" in self.previousprepids:
      result = max(result, 4)
    return result

class RedoForceCompletedSampleBase(RedoSampleGlobalBase):
  @property
  def variations(self): return super(RedoForceCompletedSampleBase, self).variations + ("Redo",)
  def __init__(self, *args, **kwargs):
    prepidtouse = kwargs.pop("prepidtouse", None)
    super(RedoForceCompletedSampleBase, self).__init__(*args, **kwargs)
    if prepidtouse is None:
        prepidtouse = self.mainsample.prepid
    else:
        if prepidtouse not in [self.mainsample.prepid] + self.mainsample.otherprepids:
            self.mainsample.addotherprepid(prepidtouse)
    self.__prepidtouse = prepidtouse
    
  @property
  def reason(self): return "because it was prematurely force completed"

  @property
  def nevents(self):
    result = super(RedoForceCompletedSampleBase, self).nevents
    if result < 1000000: return result
    try:
      finishedevents = {
        reqmgr["content"]["pdmv_evts_in_DAS"]
        for reqmgr in fullinfo(self.__prepidtouse)["reqmgr_name"]
      }
    except:
      print json.dumps(fullinfo(self.__prepidtouse), sort_keys=True, indent=4, separators=(',', ': '))
      raise
    if len(finishedevents) > 1:
      raise ValueError("More than one value for pdmv_evts_in_DAS - take a look below:\n"+json.dumps(fullinfo(self.__prepidtouse), sort_keys=True, indent=4, separators=(',', ': ')))
    result -= finishedevents.pop()
    return result

@cache
def MakeRedoForceCompletedSample(basecls):
  class RedoForceCompletedSample(RedoForceCompletedSampleBase, MakeRedoSampleBase(basecls)): pass
  RedoForceCompletedSample.__name__ = "RedoForceCompleted"+basecls.__name__
  return RedoForceCompletedSample

class RedoForceCompletedQQZZSampleRun2(MakeRedoForceCompletedSample(QQZZMCSampleRun2)):
  @classmethod
  def allsamples(cls):
    yield cls(2018, "4l")
    yield cls(2018, "2l2nu")

class ExtensionOfRedoForceCompletedQQZZSampleRun2(ExtensionOfQQZZSampleBase, MakeExtensionSample(RedoForceCompletedQQZZSampleRun2)):
  @classmethod
  def allsamples(cls):
    yield cls(2018, "4l")
    yield cls(2018, "2l2nu")

class RedoForceCompletedQQZZExtensionSampleRun2(MakeRedoForceCompletedSample(ExtensionOfQQZZSampleRun2)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, "4l", prepidtouse="HIG-RunIIFall17wmLHEGS-02149")

class RedoForceCompletedPOWHEGJHUGenMassScanMCSampleRun2(MakeRedoForceCompletedSample(POWHEGJHUGenMassScanMCSampleRun2)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, 'ggH', '4l', '450', prepidtouse="HIG-RunIIFall17wmLHEGS-02123")

class RunIIFall17DRPremix_nonsubmittedBase(RedoSampleGlobalBase):
  def __init__(self, *args, **kwargs):
    super(RunIIFall17DRPremix_nonsubmittedBase, self).__init__(*args, **kwargs)
    if "HIG-RunIIFall17wmLHEGS-00298" in self.previousprepids: assert False
  @property
  def variations(self): return super(RunIIFall17DRPremix_nonsubmittedBase, self).variations + ("RunIIFall17DRPremix_nonsubmitted",)
  @property
  def reason(self): return "RunIIFall17DRPremix_nonsubmitted"

  @classmethod
  @cacheaslist
  def allsamples(cls):
    if cls.mainsampletype is None: return

    requests = []
    Request = namedtuple("Request", "dataset prepid url")
    with open(os.path.join(here, "data", "ListRunIIFall17DRPremix_nonsubmitted.txt")) as f:
      next(f); next(f); next(f)  #cookie and header
      for line in f:
        requests.append(Request(*line.split()))
    with open(os.path.join(here, "data", "ListRunIIFall17DRPremix_nonsubmitted_2.txt")) as f:
      for line in f:
        requests.append(Request(*line.split()))

    prepids = {_.prepid for _ in requests}

    for s in cls.mainsampletype.allsamples():
      if s.year != 2017: continue
      if s.prepid in prepids:
        yield cls(*s.initargs, **s.initkwargs)

  @property
  def validationtimemultiplier(self):
    result = super(RunIIFall17DRPremix_nonsubmittedBase, self).validationtimemultiplier
    if any(_ in self.previousprepids for _ in ("HIG-RunIIFall17wmLHEGS-03116", "HIG-RunIIFall17wmLHEGS-03155")):
      result = max(result, 2)
    return result

  @property
  def extensionnumber(self):
    result = super(RunIIFall17DRPremix_nonsubmittedBase, self).extensionnumber
    if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.mass == 125:
      result += 1
      if self.productionmode == "ggH":
        result += 1
    return result

  @property
  def genproductionscommit(self):
    if isinstance(self, POWHEGJHUGenAnomCoupMCSample) and self.productionmode == "ggH": return "7e0e1d97b576734eaef5ec63c821c9ab7fb7faed"
    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ggH" and self.mass in (125, 190, 200): return "7e0e1d97b576734eaef5ec63c821c9ab7fb7faed"
    return super(RunIIFall17DRPremix_nonsubmittedBase, self).genproductionscommit

  @property
  def genproductionscommitforfragment(self):
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def tarballversion(self):
    v = super(RunIIFall17DRPremix_nonsubmittedBase, self).tarballversion

    if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass not in (125, 165, 170): v+=1  #removing some pdfs
    if not isinstance(self, PythiaVariationSampleBase):
      if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass in (120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 175, 180, 190, 200, 210, 250, 270, 300, 400, 450, 550, 600, 700, 1000, 2000, 2500, 3000): v+=1 #try multicore
      if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass in (120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 175, 180, 190, 200, 210, 250, 270, 300, 400, 450, 550, 600, 700, 1000, 2000, 2500, 3000): v+=1 #xargs instead of parallel
    if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ttH" and self.decaymode == "4l" and self.mass == 140: v+=1  #tweak seed to avoid fluctuation in filter efficiency
    if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass in (400, 3000): v+=1 #trying multicore in runcmsgrid.sh, copied the previous one too early

    if isinstance(self, JHUGenJHUGenAnomCoupMCSampleRun2) and self.productionmode == "VBF" and self.decaymode == "4l" and self.coupling == "L1Zg": v+=1

    if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ggH" and self.mass in (125, 190, 200): v+=1
    if isinstance(self, POWHEGJHUGenAnomCoupMCSampleRun2) and self.productionmode == "ggH": v+=1
    if isinstance(self, POWHEGJHUGenAnomCoupMCSampleRun2) and self.productionmode == "ggH" and self.coupling == "L1": v+=1 #corrupt copy
    if isinstance(self, POWHEGJHUGenMassScanMCSampleRun2) and self.productionmode == "ggH" and self.mass == 190: v+=1 #something got messed up

    return v

  @property
  def JHUGenversion(self):
    if isinstance(self, JHUGenJHUGenAnomCoupMCSampleRun2) and self.productionmode == "VBF" and self.decaymode == "4l" and self.coupling == "L1Zg": return "v7.2.7"
    return super(RunIIFall17DRPremix_nonsubmittedBase, self).JHUGenversion

@cache
def MakeRunIIFall17DRPremix_nonsubmitted(basecls):
  class RunIIFall17DRPremix_nonsubmitted(RunIIFall17DRPremix_nonsubmittedBase, MakeRedoSampleBase(basecls)): pass
  RunIIFall17DRPremix_nonsubmitted.__name__ = "RunIIFall17DRPremix_nonsubmitted"+basecls.__name__
  return RunIIFall17DRPremix_nonsubmitted

RunIIFall17DRPremix_nonsubmittedJHUGenJHUGenAnomalous = MakeRunIIFall17DRPremix_nonsubmitted(JHUGenJHUGenAnomCoupMCSampleRun2)
RunIIFall17DRPremix_nonsubmittedPOWHEGJHUGenAnomalous = MakeRunIIFall17DRPremix_nonsubmitted(POWHEGJHUGenAnomCoupMCSampleRun2)
RunIIFall17DRPremix_nonsubmittedJHUGenJHUGenMassScan = MakeRunIIFall17DRPremix_nonsubmitted(JHUGenJHUGenMassScanMCSampleRun2)
RunIIFall17DRPremix_nonsubmittedPOWHEGJHUGenMassScan = MakeRunIIFall17DRPremix_nonsubmitted(POWHEGJHUGenMassScanMCSampleRun2)
RunIIFall17DRPremix_nonsubmittedMCFMAnomalous = MakeRunIIFall17DRPremix_nonsubmitted(MCFMAnomCoupMCSampleRun2)

class ExtensionOfFall17NonSubJHUGenJHUGenAnomalousCouplings(MakeExtensionSample(RunIIFall17DRPremix_nonsubmittedJHUGenJHUGenAnomalous)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, "HJJ", "4l", 125, "SM")
    yield cls(2017, "HJJ", "4l", 125, "a3")
    yield cls(2017, "HJJ", "4l", 125, "a3mix")

  @property
  def nevents(self):
    if productionmode == "HJJ":
      return 1500000 - 250000


class PythiaVariationSampleBase(VariationSampleBase):
  def __init__(self, *args, **kwargs):
    self.__pythiavariation = kwargs.pop("pythiavariation")
    super(PythiaVariationSampleBase, self).__init__(*args, **kwargs)
  @property
  def pythiavariation(self):
    return self.__pythiavariation
  @property
  def variations(self): return super(PythiaVariationSampleBase, self).variations + (self.__pythiavariation,)
  @property
  def initkwargs(self): return {"pythiavariation": self.pythiavariation}
  @property
  def datasetname(self):
    result = self.mainsample.datasetname
    if self.pythiavariation != "ScaleExtension":
      result = result.replace("13TeV", "13TeV_"+self.pythiavariation.lower())
      assert self.pythiavariation.lower() in result
    return result
  @property
  def tarballversion(self):
    result = super(PythiaVariationSampleBase, self).tarballversion
    if "HIG-RunIIFall17wmLHEGS-00509" in self.previousprepids: result += 2
    if "HIG-RunIIFall17wmLHEGS-00510" in self.previousprepids: result += 2
    if "HIG-RunIIFall17wmLHEGS-01145" in self.previousprepids: result -= 2  #this one finished, the main one was reset
    if "HIG-RunIIFall17wmLHEGS-00917" in self.previousprepids: result += 2
    return result
  @property
  def nevents(self):
    if isinstance(self, POWHEGJHUGenMassScanMCSample):
      if self.productionmode in ("ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH") and self.mass == 125 and self.decaymode == "4l":
        if self.pythiavariation == "ScaleExtension":
          return 1000000
        else:
          return 500000
    if isinstance(self, MINLOMCSample):
      if self.mass in (125, 300):
        return 1000000
    raise ValueError("No nevents for {}".format(self))
  @property
  def tags(self):
    result = ["HZZ"]
    if self.year == 2017: result.append("Fall17P2A")
    return result
  @property
  def fragmentname(self):
    superfragment = result = super(PythiaVariationSampleBase, self).fragmentname
    if self.pythiavariation == "TuneUp":
      result = result.replace("CP5", "CP5Up")
    elif self.pythiavariation == "TuneDown":
      result = result.replace("CP5", "CP5Down")
    elif self.pythiavariation == "ScaleExtension":
      pass
    else:
      assert False

    if self.pythiavariation != "ScaleExtension":
      assert result != superfragment
    return result
  @property
  def genproductionscommit(self):
    return "2b8965cf8a27822882b5acdcd39282361bf07961"
    assert False, self
  @property
  def extensionnumber(self):
    result = super(PythiaVariationSampleBase, self).extensionnumber
    if self.pythiavariation == "ScaleExtension": result += 1
    return result

  @classmethod
  def allsamples(cls):
    for nominal in cls.nominalsamples():
      for systematic in "TuneUp", "TuneDown", "ScaleExtension":
        if isinstance(nominal, MINLOMCSampleRun2) and systematic == "ScaleExtension": continue
        if nominal.year != 2017 and systematic == "ScaleExtension": continue
        yield cls(*nominal.initargs, pythiavariation=systematic)

  @abstractclassmethod
  def nominalsamples(cls): return ()

@cache
def MakePythiaVariationSample(basecls):
  class PythiaVariationSample(PythiaVariationSampleBase, MakeVariationSample(basecls)): pass
  PythiaVariationSample.__name__ = "PythiaVariationOf"+basecls.__name__
  return PythiaVariationSample

class PythiaVariationPOWHEGJHUGenRun2(MakePythiaVariationSample(POWHEGJHUGenMassScanMCSampleRun2)):
  @classmethod
  def nominalsamples(cls):
    for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
      yield POWHEGJHUGenMassScanMCSampleRun2(2017, productionmode, "4l", 125)
      yield POWHEGJHUGenMassScanMCSampleRun2(2018, productionmode, "4l", 125)

class PythiaVariationMINLORun2(MakePythiaVariationSample(MINLOMCSampleRun2)):
  @classmethod
  def nominalsamples(cls):
    for sample in MINLOMCSampleRun2.allsamples():
      yield sample

class RedoPythiaVariationPOWHEGJHUGenRun2(MakeRedoSample(PythiaVariationPOWHEGJHUGenRun2)):
  @classmethod
  def allsamples(cls):
    for systematic in "TuneUp", "TuneDown":
      for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
        if productionmode == "ZH" and systematic == "TuneUp": continue
        yield cls(2017, productionmode, "4l", 125, pythiavariation=systematic, reason="wrong tune variation settings\n\nhttps://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/5361/1/1/1/2/1/1/1/2/1.html")

class RedoPythiaVariationMINLORun2(MakeRedoSample(PythiaVariationMINLORun2)):
  @classmethod
  def allsamples(cls):
    for systematic in "TuneUp", "TuneDown":
      for mass in 125, 300:
        if mass == 125 or systematic == "TuneUp": continue
        yield cls(2017, "4l", mass, pythiavariation=systematic, reason="wrong tune variation settings\n\nhttps://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/5361/1/1/1/2/1/1/1/2/1.html")

  @property
  def tarballversion(self):
    result = super(RedoPythiaVariationMINLO, self).tarballversion
    if "HIG-RunIIFall17wmLHEGS-01145" in self.previousprepids and self.pythiavariation == "TuneUp": result += 2  #parallelize the gridpack
    return result

RunIIFall17DRPremix_nonsubmittedPythiaVariation = MakeRunIIFall17DRPremix_nonsubmitted(PythiaVariationPOWHEGJHUGenRun2)
