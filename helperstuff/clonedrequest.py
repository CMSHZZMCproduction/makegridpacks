from collections import namedtuple
import os

from jobsubmission import jobtype
from utilities import cache, cacheaslist, here
from rest import McM

from mcsamplebase import MCSampleBase

class ClonedRequest(MCSampleBase):
  def __init__(self, year, originalprepid, newcampaign):
    self.originalprepid = originalprepid
    self.newcampaign = newcampaign

    super(ClonedRequest, self).__init__(year=year)

    if self.filterefficiency is None:
      assert self.originalfullinfo["generator_parameters"][0]["filter_efficiency"] == 1, self.originalfullinfo["generator_parameters"][0]["filter_efficiency"]
      self.filterefficiency = self.originalfullinfo["generator_parameters"][0]["match_efficiency"]
    if self.filterefficiencyerror is None:
      self.filterefficiencyerror = self.originalfullinfo["generator_parameters"][0]["match_efficiency_error"]
    if self.timeperevent is None:
      self.timeperevent = self.originalfullinfo["time_event"][0]
    if self.sizeperevent is None:
      self.sizeperevent = self.originalfullinfo["size_event"][0]

  @property
  def initargs(self): return self.year, self.originalprepid, self.newcampaign

  @property
  def campaign(self): return self.newcampaign

  @property
  @cache
  def originalfullinfo(self):
    result = McM().get("requests", query="prepid="+self.originalprepid)
    if not result:
      raise ValueError("mcm query for prepid="+self.originalprepid+" returned None!")
    if len(result) == 0:
      raise ValueError("mcm query for prepid="+self.originalprepid+" returned nothing!")
    if len(result) > 1:
      raise ValueError("mcm query for prepid="+self.originalprepid+" returned multiple results!")
    return result[0]

  @property
  def identifiers(self):
    return "clone", self.originalprepid, self.newcampaign
  @property
  def tarballversion(self): raise AttributeError
  def cvmfstarball_anyversion(self, version): raise AttributeError
  @property
  def foreostarball(self): return "/nonexistent/path"
  @property
  def cvmfstarballexists(self): return True
  @property
  def tmptarballbasename(self): assert False
  @property
  def makegridpackcommand(self): assert False
  @property
  def makinggridpacksubmitsjob(self): assert False
  @property
  def hasfilter(self): assert False
  @property
  def cardsurl(self): assert False
  @property
  def fragmentname(self): assert False
  @property
  def genproductionscommit(self): assert False
  @property
  def makegridpackscriptstolink(self): assert False

  @property
  def defaulttimeperevent(self): return None

  @property
  def datasetname(self):
    result = self.originalfullinfo["dataset_name"]
    if result == "ZZTo4L_14TeV_powheg_pythia8_v2": return "ZZTo4L_14TeV_powheg_pythia8"
    return result
  @property
  def fullfragment(self):
    return self.originalfullinfo["fragment"]
  @property
  def generators(self):
    return self.originalfullinfo["generators"]
  @property
  def productiongenerators(self): assert False
  @property
  def decaygenerators(self): assert False
  @property
  def xsec(self):
    return self.originalfullinfo["generator_parameters"][0]["cross_section"]
  @property
  def nthreads(self):
    if self.campaign == "PhaseIISummer17wmLHEGENOnly" and int(self.originalprepid.split("-")[-1]) in (1, 2, 3, 4, 50, 51, 35) \
     and "-".join(self.originalprepid.split("-")[:-1]) == "HIG-PhaseIITDRFall17wmLHEGS":
      return 1
    return self.originalfullinfo["sequences"][0]["nThreads"]
  @property
  def keepoutput(self):
    return False #self.originalfullinfo["keep_output"][0]
  @property
  def tags(self):
    return self.originalfullinfo["tags"]
  @property
  def notes(self):
    return self.originalfullinfo["notes"]
  @property
  def validationtimemultiplier(self):
    return self.originalfullinfo["validation"].get("time_multiplier", 1)
  @property
  def extension(self):
    if self.newcampaign not in self.originalprepid: return 0
    assert False

  @property
  def nevents(self):
    if (self.originalprepid, self.newcampaign) == ("HIG-RunIIFall17wmLHEGS-00304", "RunIISpring18wmLHEGS"): return 10000000
    if (self.originalprepid, self.newcampaign) == ("BTV-RunIIFall17wmLHEGS-00006", "RunIISpring18wmLHEGS"): return 50000000
    if self.newcampaign == "PhaseIISummer17wmLHEGENOnly":
      if self.datasetname == "ZZTo4L_14TeV_powheg_pythia8": return 26800000 #!!!
      else: return 1000000
    assert False
  @property
  def responsible(self):
    assert False, self
  @classmethod
  @cacheaslist
  def allsamples(cls):
    return ()

  def createrequest(self, clonequeue):
    self.needsupdate = True
    return clonequeue.add(self, self.pwg, self.newcampaign)

    if jobtype(): return "run locally to submit to McM"
    mcm = McM()
    clone_req = mcm.get('requests', self.originalprepid)
    clone_req['member_of_campaign'] = self.campaign
    answer = mcm.clone(self.originalprepid, clone_req)
    if not (answer and answer.get("results")):
      raise RuntimeError("Failed to create the request on McM\n{}\n{}".format(self, answer))
    self.getprepid()
    if self.prepid != answer["prepid"]:
      raise RuntimeError("Wrong prepid?? {} {}".format(self.prepid, answer["prepid"]))
    self.updaterequest()
    return "cloned request "+self.originalprepid+" as "+self.prepid+" on McM"

  def getprepid(self):
    super(ClonedRequest, self).getprepid()
    if self.prepid: return
    if jobtype(): return
    query = "dataset_name={}&extension={}&prepid={}-{}-*".format(self.originalfullinfo["dataset_name"], self.extensionnumber, self.pwg, self.campaign)
    output = McM().get('requests', query=query)
    prepids = {_["prepid"] for _ in output}
    if not prepids:
      return None
    if len(prepids) != 1:
      raise RuntimeError("Multiple prepids for {} ({})".format(self, self.datasetname, query))
    assert len(prepids) == 1, prepids
    self.prepid = prepids.pop()

  def handle_request_fragment_check_warning(self, line):
    if self.originalprepid == "BTV-RunIIFall17wmLHEGS-00006":
      if line.strip() == "* [WARNING] Are you sure you want to use CMSSW_10_0_3release which is not standard": return "ok"
      if line.strip() == "* [WARNING] This is a MadGraph LO sample with Jet matching sample. Please check": return "ok"
      if line.strip() == "* [WARNING] Do you really want to have tune CP5 in this campaign?": return "ok"
    return super(ClonedRequest, self).handle_request_fragment_check_warning(line)
