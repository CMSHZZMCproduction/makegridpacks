from utilities import cache, LSB_JOBID, restful

from mcsamplebase import MCSampleBase

class ClonedRequest(MCSampleBase):
  def __init__(self, originalprepid, newcampaign):
    self.originalprepid = originalprepid
    self.newcampaign = newcampaign

    if self.matchefficiency is None:
      assert self.originalfullinfo["generator_parameters"][0]["filter_efficiency"] == 1, self.originalfullinfo["generator_parameters"][0]["filter_efficiency"]
      self.matchefficiency = self.originalfullinfo["generator_parameters"][0]["match_efficiency"]
    if self.matchefficiencyerror is None:
      self.matchefficiencyerror = self.originalfullinfo["generator_parameters"][0]["match_efficiency_error"]
    if self.timeperevent is None:
      self.timeperevent = self.originalfullinfo["time_event"][0]
    if self.sizeperevent is None:
      self.sizeperevent = self.originalfullinfo["size_event"][0]

  @property
  def campaign(self): return self.newcampaign

  @property
  @cache
  def originalfullinfo(self):
    result = restful().getA("requests", query="prepid="+self.originalprepid)
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
  def tarballversion(self): assert False
  @property
  def cvmfstarball(self): assert False
  @property
  def foreostarball(self): return "/nonexistent/path"
  @property
  def cvmfstarballexists(self): return True
  @property
  def tmptarball(self): assert False
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
    return self.originalfullinfo["dataset_name"]
  @property
  def fullfragment(self):
    return self.originalfullinfo["fragment"]
  @property
  def generators(self):
    return self.originalfullinfo["generators"]
  @property
  def xsec(self):
    return self.originalfullinfo["generator_parameters"][0]["cross_section"]
  @property
  def nthreads(self):
    return self.originalfullinfo["sequences"][0]["nThreads"]
  @property
  def keepoutput(self):
    return self.originalfullinfo["keep_output"][0]
  @property
  def tags(self):
    return self.originalfullinfo["tags"]
  @property
  def doublevalidationtime(self):
    return self.originalfullinfo["validation"].get("double_time", False)
  @property
  def extension(self):
    if self.newcampaign not in self.originalprepid: return 0
    assert False
  
    

  @property
  def nevents(self):
    if (self.originalprepid, self.newcampaign) == ("HIG-RunIIFall17wmLHEGS-00304", "RunIISpring18wmLHEGS"): return 10000000
    assert False
  @property
  def responsible(self):
    if (self.originalprepid, self.newcampaign) == ("HIG-RunIIFall17wmLHEGS-00304", "RunIISpring18wmLHEGS"):
      return "hroskes"
    assert False, self
  @classmethod
  def allsamples(cls):
    yield cls("HIG-RunIIFall17wmLHEGS-00304", "RunIISpring18wmLHEGS")
    yield cls("BTV-RunIIFall17wmLHEGS-00006", "RunIISpring18wmLHEGS")

  def createrequest(self):
    self.needsupdate = True
    return ("Please go to https://cms-pdmv.cern.ch/mcm/requests?prepid="+self.originalprepid
            + " and clone it into "+self.newcampaign)

    if LSB_JOBID(): return "run locally to submit to McM"
    mcm = restful()
    clone_req = mcm.getA('requests', self.originalprepid)
    clone_req['member_of_campaign'] = self.campaign
    answer = mcm.clone(self.originalprepid, clone_req)
    if not (answer and answer.get("results")):
      raise RuntimeError("Failed to create the request on McM\n{}\n{}".format(self, answer))
    self.getprepid()
    if self.prepid != answer["prepid"]:
      raise RuntimeError("Wrong prepid?? {} {}".format(self.prepid, answer["prepid"]))
    self.updaterequest()
    return "cloned request "+self.originalprepid+" as "+self.prepid+" on McM"
