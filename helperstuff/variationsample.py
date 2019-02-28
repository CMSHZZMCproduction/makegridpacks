from collections import namedtuple
import abc, contextlib, csv, json, os, re, subprocess

from mcsamplebase import MCSampleBase
from utilities import cache, cacheaslist, cdtemp, fullinfo, here

from gridpackbysomeoneelse import MadGraphHJJFromThomasPlusJHUGen
from jhugenjhugenanomalouscouplings import JHUGenJHUGenAnomCoupMCSample
from jhugenjhugenmassscanmcsample import JHUGenJHUGenMassScanMCSample
from mcfmanomalouscouplings import MCFMAnomCoupMCSample
from powhegjhugenanomalouscouplings import POWHEGJHUGenAnomCoupMCSample
from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
from qqZZmcsample import QQZZMCSample

class VariationSampleBase(MCSampleBase):
  @abc.abstractproperty
  def mainsample(self): pass
  @property
  def mainmainsample(self):
    if hasattr(self.mainsample, "mainsample"):
      return self.mainsample.mainsample
    return self.mainsample
  @classmethod
  def allsamples(cls): return ()
  @abc.abstractmethod
  def mainsampletype(cls): "gets overridden in MakeVariationSample"

@cache
def MakeVariationSample(basecls):
  class VariationSample(VariationSampleBase, basecls):
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
      return basecls(self.mainsampleinitargs)
    #initargs is inherited from basecls and can be overridden
    @property
    def mainsampleinitargs(self):
      return super(VariationSample, self).initargs
    @property
    def identifiers(self):
      return super(VariationSample, self).identifiers + (self.variation,)

    def findfilterefficiency(self):
      return "this is a variation sample, the filter efficiency is the same as for the main sample"
    @property
    def defaulttimeperevent(self):
      if self.mainsample.timeperevent is not None:
        return self.mainsample.timeperevent
      return super(VariationSample, self).defaulttimeperevent

    @abc.abstractproperty
    def variation(self): pass

  return VariationSample

class ExtensionSampleGlobalBase(VariationSampleBase): pass

@cache
def MakeExtensionSampleBase(basecls):
  class ExtensionSampleBase(ExtensionSampleGlobalBase, MakeVariationSample(basecls)):
    @property
    def extensionnumber(self): return super(ExtensionSampleBase, self).extensionnumber+1
    @abc.abstractproperty
    def nevents(self): return super(ExtensionSampleBase, self).nevents
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
    def variation(self): return "ext"

    @property
    def notes(self):
      if isinstance(self.mainsample, RunIIFall17DRPremix_nonsubmittedBase): return self.mainmainsample.notes
      return super(ExtensionSample, self).notes
  return ExtensionSample

class ExtensionOfQQZZSample(MakeExtensionSample(QQZZMCSample)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, "4l")

  def handle_request_fragment_check_warning(self, line):
    from qqZZmcsample import QQZZMCSample
    if self.year == 2018 and self.finalstate == "4l":
      if line.strip() == "* [WARNING] Is 100000000 events what you really wanted - please check!":
        #yes it is
        return "ok"
    return super(ExtensionOfRedoQQZZSample, self).handle_request_fragment_check_warning(line)

  @property
  def nevents(self):
    if self.cut is None:
      if self.year == 2018 or self.year == 2017:
        if self.finalstate == "4l": return 100000000
        if self.finalstate == "2l2nu": return 50000000

    assert False, self

class ExtensionOfJHUGenJHUGenAnomalousCouplings(MakeExtensionSample(JHUGenJHUGenAnomCoupMCSample)):
  @classmethod
  def allsamples(cls):
    yield cls(2018, "HJJ", "4l", 125, "SM")
    yield cls(2018, "HJJ", "4l", 125, "a3")
    yield cls(2018, "HJJ", "4l", 125, "a3mix")
  @property
  def nevents(self):
    if productionmode == "HJJ":
      return 1500000 - 250000

class ExtensionOfMadGraphHJJFromThomasPlusJHUGen(MakeExtensionSample(MadGraphHJJFromThomasPlusJHUGen)):
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
  @abc.abstractproperty
  def variationname(self): "can be a class variable"
  @property
  def nevents(self):
    #in an earlier base class this is abstract.  not anymore.
    return super(RedoSampleGlobalBase, self).nevents
  @property
  def notes(self):
    result = "Redo of " + self.mainsample.prepid
    if self.reason is not None: result += " "+self.__reason
    supernotes = super(RedoSampleGlobalBase, self).notes
    return "\n\n".join(_ for _ in (result, supernotes) if _)

@cache
def MakeRedoSampleBase(basecls):
  class RedoSampleBase(RedoSampleGlobalBase, MakeExtensionSampleBase(basecls)): pass
  return RedoSampleBase

class RedoSampleBase(RedoSampleGlobalBase):
  variationname = "Redo"
  def __init__(self, *args, **kwargs):
    self.__reason = kwargs.pop("reason", None)
    super(RedoSampleBase, self).__init__(*args, **kwargs)
  @property
  def reason(self):
    return self.__reason


@cache
def MakeRedoSample(basecls):
  class RedoSample(RedoSampleBase, MakeRedoSampleBase(basecls)): pass
  return RedoSample

class RedoPOWHEGJHUGenMassScan(MakeRedoSample(POWHEGJHUGenMassScanMCSample)):
  @classmethod
  def allsamples(cls):
    for productionmode in "VBF", "ttH":
      yield cls(2016, productionmode, "4l", 125, reason="to check compatibility between MiniAODv2 and v3\n\nhttps://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/5977/1.html")

  @property
  def validationtimemultiplier(self):
    result = super(RedoSample, self).validationtimemultiplier
    if self.mainsample.prepid == "HIG-RunIISummer15wmLHEGS-01906":
      result = max(result, 4)
    return result

class RedoForceCompletedSampleBase(RedoSampleGlobalBase):
  variationname = "Redo"
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
  return RedoForceCompletedSample

class RedoForceCompletedQQZZSample(MakeRedoForceCompletedSample(QQZZMCSample)):
  @classmethod
  def allsamples(cls):
    yield cls(2018, "4l")
    yield cls(2018, "2l2nu")

class ExtensionOfRedoForceCompletedQQZZSample(MakeExtensionSample(RedoForceCompletedQQZZSample), ExtensionOfQQZZSample):
  @classmethod
  def allsamples(cls):
    yield cls(2018, "4l")
    yield cls(2018, "2l2nu")

class RedoForceCompletedQQZZExtensionSample(MakeRedoForceCompletedSample(ExtensionOfQQZZSample)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, "4l", prepidtouse="HIG-RunIIFall17wmLHEGS-02149")

class RedoForceCompletedPOWHEGJHUGenMassScanMCSample(MakeRedoForceCompletedSample(POWHEGJHUGenMassScanMCSample)):
  @classmethod
  def allsamples(cls):
    yield cls(2017, 'ggH', '4l', '450', prepidtouse="HIG-RunIIFall17wmLHEGS-02123")

class RedoMCFMMoreNcalls(MakeRedoSampleBase(MCFMAnomCoupMCSample)):
  variationname = "morencalls"

  @property
  def reason(self): return "increase ncalls in the phase space generation"

  @classmethod
  @cacheaslist
  def allsamples(cls):
    for sample in MCFMAnomCoupMCSample.allsamples():
      if sample.year == 2017:
        yield cls(sample.initargs)
      if sample.year == 2018 and sample.signalbkgbsi == "BKG":
        yield cls(sample.initargs)

  @property
  def tarballversion(self):
    v = self.mainsample.tarballversion+1

    if self.year == 2017:
      othersample = MCFMAnomCoupMCSample(2018, self.mainsample.signalbkgbsi, self.mainsample.width, self.mainsample.coupling, self.mainsample.finalstate)
      if self.mainsample.signalbkgbsi == "BKG":
        othersample = RedoMCFMMoreNcalls(othersample.initargs)
      assert v == othersample.tarballversion, (v, othersample.tarballversion)

    return v

  @property
  def genproductionscommit(self):
    return "a8ea4bc76df07ee2fa16bd9a67b72e7b648dec64"

  def createtarball(self, *args, **kwargs):
    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.mainsample.cvmfstarball])
      with open("readInput.DAT") as f:
        for line in f:
          if "ncalls" in line:
            assert int(line.split()[0]) < 1000000, (self, self.mainsample.cvmfstarball, line)
    return super(RedoMCFMMoreNcalls, self).createtarball(*args, **kwargs)

  @property
  def cardsurl(self):
    with open("readInput.DAT") as f:
      for line in f:
        if "ncalls" in line and int(line.split()[0]) != 5000000:
          raise ValueError(line+"\nshould be 5000000")
    return super(RedoMCFMMoreNcalls, self).cardsurl

  @property
  def extensionnumber(self):
    result = super(RedoMCFMMoreNcalls, self).extensionnumber
    if any(_.datasetname == self.datasetname and _.year == self.year and _.extensionnumber == result for _ in RunIIFall17DRPremix_nonsubmitted.allsamples()):
      result += 1
    return result

class RunIIFall17DRPremix_nonsubmittedBase(RedoSampleGlobalBase):
  variationname = "RunIIFall17DRPremix_nonsubmitted"
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
        yield cls(s.initargs)

  @property
  def validationtimemultiplier(self):
    result = super(RunIIFall17DRPremix_nonsubmittedBase, self).validationtimemultiplier
    if self.prepid in ("HIG-RunIIFall17wmLHEGS-03116", "HIG-RunIIFall17wmLHEGS-03155"):
      result = max(result, 2)
    return result

  @property
  def extensionnumber(self):
    from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
    result = super(RunIIFall17DRPremix_nonsubmittedBase, self).extensionnumber
    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.mass == 125:
      result += 1
      if self.productionmode == "ggH":
        result += 1
    return result

  @property
  def genproductionscommitforfragment(self):
    return "fd7d34a91c3160348fd0446ded445fa28f555e09"

  @property
  def tarballversion(self):
    v = super(RunIIFall17DRPremix_nonsubmittedBase, self).tarballversion
    from powhegjhugenmassscanmcsample import POWHEGJHUGenMassScanMCSample
    from jhugenjhugenanomalouscouplings import JHUGenJHUGenAnomCoupMCSample

    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass not in (125, 165, 170): v+=1  #removing some pdfs
    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass in (120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 175, 180, 190, 200, 210, 250, 270, 300, 400, 450, 550, 600, 700, 1000, 2000, 2500, 3000): v+=1 #try multicore
    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ttH" and self.decaymode == "4l" and self.mass == 140: v+=1  #tweak seed to avoid fluctuation in filter efficiency
    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass in (400, 3000): v+=1 #trying multicore in runcmsgrid.sh, copied the previous one too early
    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ZH" and self.decaymode == "4l" and self.mass in (120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 175, 180, 190, 200, 210, 250, 270, 300, 400, 450, 550, 600, 700, 1000, 2000, 2500, 3000): v+=1 #xargs instead of parallel

    if isinstance(self, JHUGenJHUGenAnomCoupMCSample) and self.productionmode == "VBF" and self.decaymode == "4l" and self.coupling == "L1Zg": v+=1

    if isinstance(self, POWHEGJHUGenMassScanMCSample) and self.productionmode == "ggH" and self.mass in (190, 200): v+=1

    return v

@cache
def MakeRunIIFall17DRPremix_nonsubmitted(basecls):
  class RunIIFall17DRPremix_nonsubmitted(RunIIFall17DRPremix_nonsubmittedBase, MakeRedoSampleBase(basecls)): pass
  return RunIIFall17DRPremix_nonsubmitted

RunIIFall17DRPremix_nonsubmittedJHUGenJHUGenAnomalous = MakeRunIIFall17DRPremix_nonsubmitted(JHUGenJHUGenAnomCoupMCSample)
RunIIFall17DRPremix_nonsubmittedPOWHEGJHUGenAnomalous = MakeRunIIFall17DRPremix_nonsubmitted(POWHEGJHUGenAnomCoupMCSample)
RunIIFall17DRPremix_nonsubmittedJHUGenJHUGenMassScan = MakeRunIIFall17DRPremix_nonsubmitted(JHUGenJHUGenMassScanMCSample)
RunIIFall17DRPremix_nonsubmittedPOWHEGJHUGenMassScan = MakeRunIIFall17DRPremix_nonsubmitted(POWHEGJHUGenMassScanMCSample)
RunIIFall17DRPremix_nonsubmittedMCFMAnomalous = MakeRunIIFall17DRPremix_nonsubmitted(MCFMAnomCoupMCSample)
RunIIFall17DRPremix_nonsubmittedPythiaVariation = MakeRunIIFall17DRPremix_nonsubmitted(PythiaVariationPOWHEGJHUGen)

class ExtensionOfFall17NonSubJHUGenJHUGenAnomalousCouplings(MakeExtensionSample(RunIIFall17DRPremix_nonsubmittedJHUGenJHUGenAnomalous), ExtensionOfJHUGenJHUGenAnomalousCouplings):
  @classmethod
  def allsamples(cls):
    yield cls(2017, "HJJ", "4l", 125, "SM")
    yield cls(2017, "HJJ", "4l", 125, "a3")
    yield cls(2017, "HJJ", "4l", 125, "a3mix")



class PythiaVariationSampleBase(VariationSampleBase):
  @property
  def datasetname(self):
    result = self.mainsample.datasetname
    if self.variation != "ScaleExtension":
      result = result.replace("13TeV", "13TeV_"+self.variation.lower())
      assert self.variation.lower() in result
    return result
  @property
  def tarballversion(self):
    result = super(PythiaVariationSampleBase, self).tarballversion
    if self.prepid == "HIG-RunIIFall17wmLHEGS-00509": result += 2
    if self.prepid == "HIG-RunIIFall17wmLHEGS-01145": result -= 2  #this one finished, the main one was reset
    if self.mainsample.prepid == "HIG-RunIIFall17wmLHEGS-00917": result += 2
    return result
  @property
  def nevents(self):
    if isinstance(self, POWHEGJHUGenMassScanMCSample):
      if self.productionmode in ("ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH") and self.mass == 125 and self.decaymode == "4l":
        if self.variation == "ScaleExtension":
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
    if self.variation == "TuneUp":
      result = result.replace("CP5", "CP5Up")
    elif self.variation == "TuneDown":
      result = result.replace("CP5", "CP5Down")
    elif self.variation == "ScaleExtension":
      pass
    else:
      assert False

    if self.variation != "ScaleExtension":
      assert result != superfragment
    return result
  @property
  def genproductionscommit(self):
    if self.year == 2017: return "49196efff87a61016833754619a299772ba3c33d"
    if self.year == 2018: return "2b8965cf8a27822882b5acdcd39282361bf07961"
    assert False, self
  @property
  def extensionnumber(self):
    result = super(PythiaVariationSampleBase, self).extensionnumber
    if self.variation == "ScaleExtension": result += 1
    return result

  @classmethod
  def allsamples(cls):
    for nominal in cls.nominalsamples():
      for systematic in "TuneUp", "TuneDown", "ScaleExtension":
        if isinstance(nominal, MINLOMCSample) and systematic == "ScaleExtension": continue
        if nominal.year != 2017 and systematic == "ScaleExtension": continue
        yield cls(nominal, systematic)

  @abc.abstractmethod
  def nominalsamples(cls): return ()

@cache
def MakePythiaVariationSample(basecls):
  class PythiaVariationSample(PythiaVariationSampleBase, MakeVariationSampleBase(basecls)): pass
  return PythiaVariationSample

class PythiaVariationPOWHEGJHUGen(MakePythiaVariationSample(POWHEGJHUGenMassScanMCSample)):
  @classmethod
  def nominalsamples(cls):
    for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
      yield POWHEGJHUGenMassScanMCSample(2017, productionmode, "4l", 125)
      yield POWHEGJHUGenMassScanMCSample(2018, productionmode, "4l", 125)

class PythiaVariationMINLO(MakePythiaVariationSample(MINLOMCSample)):
  @classmethod
  def nominalsamples(cls):
    for sample in MINLOMCSample.allsamples():
      if sample.energy == 13:
        yield sample

class RedoPythiaVariationPOWHEGJHUGen(MakeRedoSample(PythiaVariationPOWHEGJHUGen)):
  @classmethod
  def allsamples(cls):
    for systematic in "TuneUp", "TuneDown":
      for productionmode in "ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH":
        if productionmode == "ZH" and systematic == "TuneUp": continue
        yield cls(2017, productionmode, "4l", 125, systematic, reason="wrong tune variation settings\n\nhttps://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/5361/1/1/1/2/1/1/1/2/1.html")

  @property
  def tarballversion(self):
    result = super(RedoSample, self).tarballversion
    if self.mainsample.prepid == "HIG-RunIIFall17wmLHEGS-00510": result += 2  #parallelize the gridpack
    return result

class RedoPythiaVariationMINLO(MakeRedoSample(PythiaVariationMINLO)):
  @classmethod
  def allsamples(cls):
    for systematic in "TuneUp", "TuneDown":
      for mass in 125, 300:
        if mass == 125 or systematic == "TuneUp": continue
        yield cls(2017, "4l", mass, systematic, reason="wrong tune variation settings\n\nhttps://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/5361/1/1/1/2/1/1/1/2/1.html")

  @property
  def tarballversion(self):
    result = super(RedoSample, self).tarballversion
    if self.mainsample.prepid == "HIG-RunIIFall17wmLHEGS-01145": result += 2  #parallelize the gridpack
    return result
