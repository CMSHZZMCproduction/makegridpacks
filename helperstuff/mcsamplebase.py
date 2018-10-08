import abc, filecmp, glob, itertools, os, pycurl, re, shutil, stat, subprocess

import uncertainties

from McMScripts.manageRequests import createLHEProducer

import patches
from utilities import cache, cd, cdtemp, genproductions, here, jobended, JsonDict, KeepWhileOpenFile, LSB_JOBID, mkdir_p, queuematches, restful, submitLSF, wget

class MCSampleBase(JsonDict):
  @abc.abstractmethod
  def __init__(self, year):
    self.__year = int(year)
  @property
  def year(self):
    return self.__year
  @abc.abstractproperty
  def identifiers(self):
    """example: productionmode, decaymode, mass"""
  @abc.abstractproperty
  def tarballversion(self): pass
  @property
  def cvmfstarball(self): return self.cvmfstarball_anyversion(self.tarballversion)
  @abc.abstractmethod
  def cvmfstarball_anyversion(self, version): pass
  @abc.abstractproperty
  def tmptarball(self): pass
  @abc.abstractproperty
  def makegridpackcommand(self): pass
  @abc.abstractproperty
  def makinggridpacksubmitsjob(self):
    """returns the job name"""
  @abc.abstractproperty
  def hasfilter(self): pass
  @abc.abstractproperty
  def datasetname(self): pass
  @property
  def extensionnumber(self):
    """This should normally be 0, only change it for extension samples"""
    return 0
  @abc.abstractproperty
  def nevents(self): pass
  @property
  def generators(self):
    result = self.productiongenerators+self.decaygenerators
    newresult = []
    seen = set()
    for _ in result:
      if _ in seen: continue
      seen.add(_)
      newresult.append(_)
    return newresult
  @abc.abstractproperty
  def productiongenerators(self): return []
  @property
  def decaygenerators(self): return []
  @abc.abstractproperty
  def cardsurl(self): pass
  @abc.abstractproperty
  def defaulttimeperevent(self): pass
  @abc.abstractproperty
  def tags(self): pass
  @abc.abstractproperty
  def fragmentname(self): pass
  @abc.abstractproperty
  def genproductionscommit(self): pass
  @property
  def genproductionscommitforfragment(self): return self.genproductionscommit
  @abc.abstractproperty
  def makegridpackscriptstolink(self): pass
  @abc.abstractproperty
  def xsec(self): pass
  @abc.abstractproperty
  def responsible(self): "put the lxplus username of whoever makes these gridpacks"
  @property
  def patchkwargs(self): return None
  @property
  def doublevalidationtime(self): return False
  @property
  def neventsfortest(self): return None
  @property
  def notes(self): return ""
  @property
  def creategridpackqueue(self): return "1nd"
  @property
  def timepereventqueue(self): return "1nd"
  @property
  def filterefficiencyqueue(self): return "1nd"
  @property
  def dovalidation(self):
    """Set this to false if a request fails so badly that the validation will never succeed"""
    return True
  @property
  def inthemiddleofmultistepgridpackcreation(self):
    """powheg samples that need to run the grid in multiple steps should sometimes return true"""
    return False
  @property
  def gridpackjobsrunning(self):
    """powheg samples that need to run the grid in multiple steps need to modify this"""
    if not self.makinggridpacksubmitsjob: return False
    return not jobended("-J", self.makinggridpacksubmitsjob)
  def processmakegridpackstdout(self, stdout): "do nothing by default, powheg uses this"

  @abc.abstractmethod
  def allsamples(self): "should be a classmethod"

  def __eq__(self, other):
    return self.keys == other.keys
  def __ne__(self, other):
    return not (self == other)
  def __hash__(self):
    return hash(self.keys)
  def __str__(self):
    return " ".join(str(_) for _ in self.keys)
  def __repr__(self):
    return type(self).__name__+"(" +  ", ".join(repr(_) for _ in self.keys) + ")"

  @property
  def eostarball(self):
    return self.cvmfstarball.replace("/cvmfs/cms.cern.ch/phys_generator/", "/eos/cms/store/group/phys_generator/cvmfs/")
  @property
  def foreostarball(self):
    """to put in a directory structure here, which will later be copied to eos"""
    return self.cvmfstarball.replace("/cvmfs/cms.cern.ch/phys_generator/", here+"/")

  @cache
  def checkcardsurl(self):
    try:
      self.cardsurl
    except Exception as e:
      if str(self) in str(e):
        return str(e).replace(str(self), "").strip()
      else:
        raise

  @property
  def workdir(self):
    result = os.path.dirname(self.tmptarball)
    if os.path.commonprefix((result, os.path.join(here, "workdir"))) != os.path.join(here, "workdir"):
      raise ValueError("{!r}.workdir is supposed to be in the workdir folder".format(self))
    if result == os.path.join(here, "workdir"):
      raise ValueError("{!r}.workdir is supposed to be a subfolder of the workdir folder, not workdir itself".format(self))
    return result

  @property
  def cvmfstarballexists(self): return os.path.exists(self.cvmfstarball)

  def patchtarball(self):
    if os.path.exists(self.cvmfstarball) or os.path.exists(self.eostarball) or os.path.exists(self.foreostarball): return

    if not self.needspatch: assert False
    mkdir_p(self.workdir)
    with KeepWhileOpenFile(self.tmptarball+".tmp", message=LSB_JOBID()) as kwof:
      if not kwof:
        return "job to patch the tarball is already running"

      kwargs = self.needspatch
      if isinstance(kwargs, int):
        kwargs = self.patchkwargs
        kwargs["oldtarballversion"] = self.needspatch
      if "oldfilename" in kwargs or "newfilename" in kwargs or "sample" in kwargs: assert False, kwargs
      kwargs["oldfilename"] = self.cvmfstarball_anyversion(version=kwargs.pop("oldtarballversion"))
      kwargs["newfilename"] = self.foreostarball
      mkdir_p(os.path.dirname(self.foreostarball))

      patches.dopatch(**kwargs)

      if not os.path.exists(self.foreostarball): raise RuntimeError("Patching failed, gridpack doesn't exist")
      if self.timeperevent is not None:
        del self.timeperevent
      self.needspatch = False

      return "tarball is patched and the new version is in this directory to be copied to eos"

  def createtarball(self):
    if os.path.exists(self.cvmfstarball) or os.path.exists(self.eostarball) or os.path.exists(self.foreostarball): return

    mkdir_p(self.workdir)
    with cd(self.workdir), KeepWhileOpenFile(self.tmptarball+".tmp", message=LSB_JOBID()) as kwof:
      if not kwof:
        with open(self.tmptarball+".tmp") as f:
          try:
            jobid = int(f.read().strip())
          except ValueError:
            return "try running again, probably you just got really bad timing"
        if jobended(str(jobid)):
          if self.makinggridpacksubmitsjob:
            os.remove(self.tmptarball+".tmp")
            return "job died at a very odd time, cleaned it up.  Try running again."
          for _ in os.listdir("."):            #--> delete everything in the folder, except the tarball if that exists
            if os.path.basename(_) != os.path.basename(self.tmptarball) and os.path.basename(_) != os.path.basename(self.tmptarball)+".tmp":
              try:
                os.remove(_)
              except OSError:
                shutil.rmtree(_)
          os.remove(os.path.basename(self.tmptarball)+".tmp") #remove that last
          return "gridpack job died, cleaned it up.  run makegridpacks.py again."
        else:
          return "job to make the tarball is already running"

      if self.gridpackjobsrunning:
        return "job to make the tarball is already running"

      if not os.path.exists(self.tmptarball):
        if not self.inthemiddleofmultistepgridpackcreation:
          for _ in os.listdir("."):
            if not _.endswith(".tmp"):
              try:
                os.remove(_)
              except OSError:
                shutil.rmtree(_)
        if not self.makinggridpacksubmitsjob and self.creategridpackqueue is not None:
          if not LSB_JOBID(): return "need to create the gridpack, submitting to LSF" if submitLSF(self.creategridpackqueue) else "need to create the gridpack, job is pending on LSF"
          if not queuematches(self.creategridpackqueue): return "need to create the gridpack, but on the wrong queue"
        for filename in self.makegridpackscriptstolink:
          os.symlink(filename, os.path.basename(filename))

        makinggridpacksubmitsjob = self.makinggridpacksubmitsjob

        #https://stackoverflow.com/a/17698359/5228524
        makegridpackstdout = ""
        pipe = subprocess.Popen(self.makegridpackcommand, stdout=subprocess.PIPE, bufsize=1)
        with pipe.stdout:
            for line in iter(pipe.stdout.readline, b''):
                print line,
                makegridpackstdout += line
        self.processmakegridpackstdout(makegridpackstdout)

        if makinggridpacksubmitsjob:
          return "submitted the gridpack creation job"
        if self.inthemiddleofmultistepgridpackcreation:
          return "ran one step of gridpack creation, run again to continue"

      mkdir_p(os.path.dirname(self.foreostarball))
      if self.patchkwargs:
        kwargs = self.patchkwargs
        for _ in "oldfilename", "newfilename", "sample": assert _ not in kwargs, _
        with cdtemp():
          kwargs["oldfilename"] = self.tmptarball
          kwargs["newfilename"] = os.path.abspath(os.path.basename(self.tmptarball))
          #kwargs["sample"] = self #???
          patches.dopatch(**kwargs)
          shutil.move(os.path.basename(self.tmptarball), self.tmptarball)

      if self.timeperevent is not None:
        del self.timeperevent
      shutil.move(self.tmptarball, self.foreostarball)
      shutil.rmtree(os.path.dirname(self.tmptarball))
      return "tarball is created and moved to this folder, to be copied to eos"

  def findmatchefficiency(self):
    #figure out the filter efficiency
    if not self.hasfilter:
      self.matchefficiency = 1
      return "filter efficiency is set to 1 +/- 0"
    else:
      if not self.implementsfilter: raise ValueError("Can't find match efficiency for {.__name__} which doesn't implement filtering!".format(type(self)))
      mkdir_p(self.workdir)
      jobsrunning = False
      eventsprocessed = eventsaccepted = 0
      with cd(self.workdir):
        for i in range(100):
          mkdir_p(str(i))
          with cd(str(i)), KeepWhileOpenFile("runningfilterjob.tmp", message=LSB_JOBID(), deleteifjobdied=True) as kwof:
            if not kwof:
              jobsrunning = True
              continue
            if not os.path.exists(self.filterresultsfile):
              if not LSB_JOBID():
                submitLSF(self.filterefficiencyqueue)
                jobsrunning = True
                continue
              if not queuematches(self.filterefficiencyqueue):
                jobsrunning = True
                continue
              self.dofilterjob(i)
            processed, accepted = self.getfilterresults(i)
            eventsprocessed += processed
            eventsaccepted += accepted

        if jobsrunning: return "some filter efficiency jobs are still running"
        self.matchefficiency = uncertainties.ufloat(1.0*eventsaccepted / eventsprocessed, (1.0*eventsaccepted * (eventsprocessed-eventsaccepted) / eventsprocessed**3) ** .5)
        #shutil.rmtree(self.workdir)
        return "match efficiency is measured to be {}".format(self.matchefficiency)

  implementsfilter = False

  def getsizeandtime(self):
    mkdir_p(self.workdir)
    with KeepWhileOpenFile(os.path.join(self.workdir, self.prepid+".tmp"), message=LSB_JOBID(), deleteifjobdied=True) as kwof:
      if not kwof: return "job to get the size and time is already running"
      if not LSB_JOBID(): return "need to get time and size per event, submitting to LSF" if submitLSF(self.timepereventqueue) else "need to get time and size per event, job is pending on LSF"
      if not queuematches(self.timepereventqueue): return "need to get time and size per event, but on the wrong queue"
      with cdtemp():
        wget(os.path.join("https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_test/", self.prepid, str(self.neventsfortest) if self.neventsfortest else "").rstrip("/"), output=self.prepid)
        with open(self.prepid) as f:
          testjob = f.read()
        with open(self.prepid, "w") as newf:
          newf.write(eval(testjob))
        os.chmod(self.prepid, os.stat(self.prepid).st_mode | stat.S_IEXEC)
        subprocess.check_call(["./"+self.prepid], stderr=subprocess.STDOUT)
        with open(self.prepid+"_rt.xml") as f:
          nevents = totalsize = None
          for line in f:
            line = line.strip()
            match = re.match('<TotalEvents>([0-9]*)</TotalEvents>', line)
            if match: nevents = int(match.group(1))
            match = re.match('<Metric Name="Timing-tstoragefile-write-totalMegabytes" Value="([0-9.]*)"/>', line)
            if match: totalsize = float(match.group(1))
            if self.year >= 2017:
              match = re.match('<Metric Name="EventThroughput" Value="([0-9.eE+-]*)"/>', line)
              if match: self.timeperevent = 1/float(match.group(1))
            else:
              match = re.match('<Metric Name="AvgEventTime" Value="([0-9.eE+-]*)"/>', line)
              if match: self.timeperevent = float(match.group(1))
          if nevents is not None is not totalsize:
            self.sizeperevent = totalsize * 1024 / nevents

    shutil.rmtree(self.workdir)

    if not (self.sizeperevent and self.timeperevent):
      return "failed to get the size and time"
    if LSB_JOBID(): return "size and time per event are found to be {} and {}, run locally to send to McM".format(self.sizeperevent, self.timeperevent)
    self.updaterequest()
    return "size and time per event are found to be {} and {}, sent it to McM".format(self.sizeperevent, self.timeperevent)

  def makegridpack(self, approvalqueue, badrequestqueue, clonequeue, setneedsupdate=False):
    if self.finished: return "finished!"
    if not self.cvmfstarballexists:
      if not os.path.exists(self.eostarball):
        if not os.path.exists(self.foreostarball):
          if self.needspatch: return self.patchtarball()
          return self.createtarball()
        return "gridpack exists in this folder, to be copied to eos" 
      return "gridpack exists on eos, not yet copied to cvmfs"

    if os.path.exists(self.foreostarball):
      if filecmp.cmp(self.cvmfstarball, self.foreostarball, shallow=False):
        os.remove(self.foreostarball)
        self.needsupdate = True
      else:
        return "gridpack exists on cvmfs, but it's wrong!"

    if self.badprepid:
      badrequestqueue.add(self)

    if self.prepid is None:
      self.getprepid()
      if self.prepid is None:
        #need to make the request
        return self.createrequest(clonequeue)
      else:
        return "found prepid: {}".format(self.prepid)

    if (self.matchefficiency is None or self.matchefficiencyerror is None) and not self.needsupdate:
      return self.findmatchefficiency()

    if not (self.sizeperevent and self.timeperevent) and not self.needsupdate:
      return self.getsizeandtime()

    if LSB_JOBID():
      return "please run locally to check and/or advance the status".format(self.prepid)

    if self.badprepid:
      badrequestqueue.add(self)

    if (self.approval, self.status) == ("none", "new"):
      if self.needsoptionreset:
        if not self.optionreset():
          return "need to do option reset but failed"
        return "needed option reset, sent it to McM"
      if self.needsupdateiffailed:
        self.updaterequest()
        if self.badprepid:
          badrequestqueue.add(self)
        return "needs update on McM, sending it there"
      if not self.dovalidation: return "not starting the validation"
      if self.nthreads > 1 and self.fullinfo["history"][-1]["action"] == "failed":
        self.nthreads /= 2
        self.updaterequest()
        return "validation failed, decreasing the number of threads"
      if setneedsupdate and not self.needsupdate:
        result = self.setneedsupdate()
        if result: return result
      approvalqueue.validate(self)
      return "starting the validation"
    if (self.approval, self.status) == ("validation", "new"):
      if setneedsupdate and not self.needsupdate:
        result = self.setneedsupdate()
        if result: return result
      return "validation is running"
    if (self.approval, self.status) == ("validation", "validation"):
      self.gettimepereventfromMcM()
      if setneedsupdate and not self.needsupdate:
        result = self.setneedsupdate()
        if result: return result
      if self.needsupdate:
        approvalqueue.reset(self)
        return "needs update on McM, resetting the request"
      self.needsupdateiffailed = False
      approvalqueue.define(self)
      return "defining the request"
    if (self.approval, self.status) == ("define", "defined"):
      if self.needsupdate:
        approvalqueue.reset(self)
        return "needs update on McM, resetting the request"
      if setneedsupdate and not self.needsupdate:
        result = self.setneedsupdate()
        if result: return result
      self.needsupdateiffailed = False
      return "request is defined"
    if (self.approval, self.status) in (("submit", "approved"), ("approve", "approved")):
      if self.needsupdate:
        return "{} is already approved, but needs update!".format(self)
      self.needsupdateiffailed = False
      return "approved"
    if (self.approval, self.status) == ("submit", "submitted"):
      if self.needsupdate:
        return "{} is already submitted, but needs update!".format(self)
      self.needsupdateiffailed = False
      return "submitted"
    if (self.approval, self.status) == ("submit", "done"):
      if self.needsupdate:
        return "{} is already finished, but needs update!".format(self)
      self.needsupdateiffailed = False
      self.gettimepereventfromMcM()
      self.finished = True
      return "finished!"
    return "Unknown approval "+self.approval+" and status "+self.status

  @property
  def keepoutput(self): return False

  #these things should all be calculated once.
  #they are stored in McMsampleproperties.json in this folder.
  #see JsonDict in utilities for how that works
  @property
  def keys(self):
    return (str(self.year),) + tuple(str(_) for _ in self.identifiers)
  dictfile = "McMsampleproperties.json"
  @property
  def default(self): return {}

  @property
  def prepid(self):
    with cd(here):
      return self.value.get("prepid")
  @prepid.setter
  def prepid(self, value):
    with cd(here), self.writingdict():
      self.value["prepid"] = value
  @prepid.deleter
  def prepid(self):
    with cd(here), self.writingdict():
      del self.value["prepid"]
  @property
  def timeperevent(self):
    with cd(here):
      return self.value.get("timeperevent")
  @timeperevent.setter
  def timeperevent(self, value):
    with cd(here), self.writingdict():
      self.value["timeperevent"] = value
    self.needsupdateiffailed = True
  @timeperevent.deleter
  def timeperevent(self):
    with cd(here), self.writingdict():
      del self.value["timeperevent"]
    self.resettimeperevent = True
  @property
  def resettimeperevent(self):
    with cd(here):
      return self.value.get("resettimeperevent", False)
  @resettimeperevent.setter
  def resettimeperevent(self, value):
    if value:
      with cd(here), self.writingdict():
        self.value["resettimeperevent"] = True
    elif self.resettimeperevent:
      del self.resettimeperevent
  @resettimeperevent.deleter
  def resettimeperevent(self):
    with cd(here), self.writingdict():
      del self.value["resettimeperevent"]
  @property
  def sizeperevent(self):
    with cd(here):
      return self.value.get("sizeperevent")
  @sizeperevent.setter
  def sizeperevent(self, value):
    with cd(here), self.writingdict():
      self.value["sizeperevent"] = value
    self.needsupdateiffailed = True
  @sizeperevent.deleter
  def sizeperevent(self):
    with cd(here), self.writingdict():
      del self.value["sizeperevent"]
  @property
  def matchefficiency(self):
    if self.matchefficiencynominal is None or self.matchefficiencyerror is None: return None
    return uncertainties.ufloat(self.matchefficiencynominal, self.matchefficiencyerror)
  @matchefficiency.setter
  def matchefficiency(self, value):
    nominal, error = uncertainties.nominal_value(value), uncertainties.std_dev(value)
    if error == 0 and nominal != 1: raise ValueError("Are you sure you want to set the matchefficiency to {} with no error?".format(uncertainties.ufloat(nominal, error)))
    self.matchefficiencynominal = nominal
    self.matchefficiencyerror = error
  @matchefficiency.deleter
  def matchefficiency(self):
    del self.matchefficiencynominal, self.matchefficiencyerror
  @property
  def matchefficiencynominal(self):
    with cd(here):
      return self.value.get("matchefficiency")
  @matchefficiencynominal.setter
  def matchefficiencynominal(self, value):
    with cd(here), self.writingdict():
      self.value["matchefficiency"] = value
    self.needsupdate = True
  @matchefficiencynominal.deleter
  def matchefficiencynominal(self):
    with cd(here), self.writingdict():
      del self.value["matchefficiency"]
  @property
  def matchefficiencyerror(self):
    with cd(here):
      return self.value.get("matchefficiencyerror")
  @matchefficiencyerror.setter
  def matchefficiencyerror(self, value):
    with cd(here), self.writingdict():
      self.value["matchefficiencyerror"] = value
    self.needsupdate = True
  @matchefficiencyerror.deleter
  def matchefficiencyerror(self):
    with cd(here), self.writingdict():
      del self.value["matchefficiencyerror"]
  @property
  def needsupdate(self):
    if self.needsoptionreset:
      self.needsupdate = True
    with cd(here):
      return self.value.get("needsupdate", False)
  @needsupdate.setter
  def needsupdate(self, value):
    if value:
      with cd(here), self.writingdict():
        self.value["needsupdate"] = True
    elif self.needsupdate:
      del self.needsupdate
  @needsupdate.deleter
  def needsupdate(self):
    with cd(here), self.writingdict():
      del self.value["needsupdate"]
  @property
  def needsupdateiffailed(self):
    if self.needsupdate: return True
    with cd(here):
      return self.value.get("needsupdateiffailed", False)
  @needsupdateiffailed.setter
  def needsupdateiffailed(self, value):
    if value:
      with cd(here), self.writingdict():
        self.value["needsupdateiffailed"] = True
    else:
      if self.needsupdateiffailed:
        del self.needsupdateiffailed
      self.needsupdate = False
  @needsupdateiffailed.deleter
  def needsupdateiffailed(self):
    with cd(here), self.writingdict():
      del self.value["needsupdateiffailed"]
  @property
  def needsoptionreset(self):
    with cd(here):
      return self.value.get("needsoptionreset", False)
  @needsoptionreset.setter
  def needsoptionreset(self, value):
    if value:
      with cd(here), self.writingdict():
        self.value["needsoptionreset"] = True
    elif self.needsoptionreset:
      del self.needsoptionreset
  @needsoptionreset.deleter
  def needsoptionreset(self):
    with cd(here), self.writingdict():
      del self.value["needsoptionreset"]
  @property
  def badprepid(self):
    with cd(here):
      result = self.value.get("badprepid", [])
      #backwards compatibility
      if isinstance(result, basestring): result = [result]

      originalresult = result[:]
      for _ in result[:]:
        if not LSB_JOBID() and not restful().get("requests", _):
          result.remove(_)

      if result != originalresult:
        self.badprepid = result

      return result

  @badprepid.setter
  def badprepid(self, value):
    if value:
      with cd(here), self.writingdict():
        self.value["badprepid"] = value
    elif "badprepid" in self.value:
      del self.badprepid
  @badprepid.deleter
  def badprepid(self):
    with cd(here), self.writingdict():
      del self.value["badprepid"]
  @property
  def finished(self):
    with cd(here):
      return self.value.get("finished", False)
  @finished.setter
  def finished(self, value):
    if value:
      with cd(here), self.writingdict():
        self.value["finished"] = True
    elif self.finished:
      del self.finished
  @finished.deleter
  def finished(self):
    with cd(here), self.writingdict():
      del self.value["finished"]
  @property
  def needspatch(self):
    with cd(here):
      return self.value.get("needspatch", {})
  @needspatch.setter
  def needspatch(self, value):
    if value:
      if isinstance(value, int):
        pass
      else:
        try:
          value["oldtarballversion"]
        except TypeError:
          raise ValueError("needspatch has to be a dict or version number")
        except KeyError:
          raise ValueError('needspatch has to have "oldtarballversion" in it')
        for _ in "oldfilename", "newfilename", "sample":
          if _ in value:
            raise ValueError('''needspatch can't have "'''+_+'" in it')
        if "functionname" not in value:
          raise ValueError('needspatch has to have "functionname" in it')
        if value["functionname"] not in patches.functiondict:
          raise ValueError('invalid functionname "{functionname}", choices:\n{}'.format(", ".join(patches.functiondict), **value))
      with cd(here), self.writingdict():
        self.value["needspatch"] = value
    elif self.needspatch:
      del self.needspatch
  @needspatch.deleter
  def needspatch(self):
    with cd(here), self.writingdict():
      del self.value["needspatch"]
  @property
  def nthreads(self):
    with cd(here):
      if "nthreads" not in self.value and (self.finished or self.status in ("submitted", "approved")):
        self.value["nthreads"] = self.fullinfo["sequences"][0]["nThreads"]
      return self.value.get("nthreads", 8 if self.year >= 2017 else 1)
  @nthreads.setter
  def nthreads(self, value):
    if "nthreads" in self.value and value == self.nthreads: return
    with cd(here), self.writingdict():
      self.value["nthreads"] = int(value)
    del self.timeperevent
  @nthreads.deleter
  def nthreads(self):
    with cd(here), self.writingdict():
      del self.value["nthreads"]

  @property
  def memory(self):
    if self.nthreads == 1: return 2.3
    return 4
  @property
  def filterefficiency(self): return 1
  @property
  def filterefficiencyerror(self): return 0.1

  @property
  @cache
  def fullfragment(self):
    return createLHEProducer(self.cvmfstarball, self.cardsurl, self.fragmentname, self.genproductionscommitforfragment)

  def getdictforupdate(self):
    mcm = restful()
    req = mcm.get("requests", self.prepid)
    req["dataset_name"] = self.datasetname
    req["mcdb_id"] = 0
    req["total_events"] = self.nevents
    req["fragment"] = self.fullfragment
    req["time_event"] = [(self.timeperevent if self.timeperevent is not None else self.defaulttimeperevent)]
    req["size_event"] = [self.sizeperevent if self.sizeperevent is not None else 600]
    req["generators"] = self.generators
    if self.matchefficiency is not None:
      req["generator_parameters"][0].update({
        "match_efficiency_error": self.matchefficiency.std_dev,
        "match_efficiency": self.matchefficiency.nominal_value,
        "filter_efficiency": self.filterefficiency,
        "filter_efficiency_error": self.filterefficiencyerror,
        "cross_section": self.xsec,
      })
    req["sequences"][0]["nThreads"] = self.nthreads
    req["keep_output"][0] = bool(self.keepoutput)
    req["tags"] = self.tags
    req["memory"] = self.memory
    req["validation"].update({
      "double_time": self.doublevalidationtime,
    })
    req["extension"] = self.extensionnumber
    req["notes"] = self.notes
    return req

  def updaterequest(self):
    mcm = restful()
    req = self.getdictforupdate()
    try:
      answer = mcm.update('requests', req)
    except pycurl.error as e:
#      if e[0] == 52 and e[1] == "Empty reply from server":
#        self.badprepid += [self.prepid]
#        del self.prepid
#        return
#      else:
        raise
    if not (answer and answer.get("results")):
      raise RuntimeError("Failed to modify the request on McM\n{}\n{}".format(self, answer))
    self.needsupdate = False
    self.needsupdateiffailed = False
    self.resettimeperevent = False

  @property
  def pwg(self): return "HIG"
  @abc.abstractproperty
  def campaign(self): pass

  def createrequest(self, clonequeue):
    if LSB_JOBID(): return "run locally to submit to McM"
    mcm = restful()
    req = {
      "pwg": self.pwg,
      "member_of_campaign": self.campaign,
      "mcdb_id": 0,
      "dataset_name": self.datasetname,
      "extension": self.extensionnumber,
    }
    answer = mcm.put("requests", req)
    if not (answer and answer.get("results")):
      raise RuntimeError("Failed to create the request on McM\n{}\n\n{}\n\n{}".format(self, req, answer))
    self.getprepid()
    if self.prepid != answer["prepid"]:
      raise RuntimeError("Wrong prepid?? {} {}".format(self.prepid, answer["prepid"]))
    self.updaterequest()
    return "created request "+self.prepid+" on McM"

  def getprepid(self):
    if LSB_JOBID(): return
    query = "dataset_name={}&extension={}&prepid={}-{}-*".format(self.datasetname, self.extensionnumber, self.pwg, self.campaign)
    output = restful().get('requests', query=query)
    prepids = {_["prepid"] for _ in output}
    prepids -= frozenset(self.badprepid)
    if not prepids:
      return None
    if len(prepids) != 1:
      raise RuntimeError("Multiple prepids for {} ({})".format(self, self.datasetname, query))
    assert len(prepids) == 1, prepids
    self.prepid = prepids.pop()

  @property
  @cache
  def fullinfo(self):
    if not self.prepid: raise ValueError("Can only call fullinfo once the prepid has been set")
    result = restful().get("requests", query="prepid="+self.prepid)
    if not result:
      raise ValueError("mcm query for prepid="+self.prepid+" returned None!")
    if len(result) == 0:
      raise ValueError("mcm query for prepid="+self.prepid+" returned nothing!")
    if len(result) > 1:
      raise ValueError("mcm query for prepid="+self.prepid+" returned multiple results!")
    return result[0]

  def gettimepereventfromMcM(self):
    if self.timeperevent is None or self.resettimeperevent: return
    needsupdate = self.needsupdate
    needsupdateiffailed = self.needsupdateiffailed
    timeperevent = self.fullinfo["time_event"][0]
    sizeperevent = self.fullinfo["size_event"][0]
    try:
      if timeperevent != self.defaulttimeperevent:
        self.timeperevent = timeperevent
      self.sizeperevent = sizeperevent
    finally:
      #don't need to reupdate on McM, unless that was already necessary
      self.needsupdate = needsupdate
      self.needsupdateiffailed = needsupdateiffailed

  @property
  def approval(self):
    return self.fullinfo["approval"]
  @property
  def status(self):
    return self.fullinfo["status"]

  def delete(self):
    response = ""
    while response not in ("yes", "no"):
      response = raw_input("are you sure you want to delete {}? [yes/no]".format(self))
    if response == "no": return
    if self.prepid:
      restful().approve("requests", self.prepid, 0)
      restful().deleteA("requests", self.prepid)
    with cd(here), self.writingdict():
      del self.value

  def optionreset(self):
    if self.prepid is None: return
    self.needsupdate = True
    results = restful().get("restapi/requests/option_reset/"+self.prepid)
    try:
      success = bool(results["results"][self.prepid])
    except KeyError:
      success = False
    if success:
      self.needsoptionreset = False
    return success

  def setneedsupdate(self):
    old = self.fullinfo
    validated = self.status in ("validation", "defined", "approved", "submitted", "done")
    if validated:
      self.gettimepereventfromMcM()
    new = self.getdictforupdate()
    different = set()
    differentsequences = set()
    for key in set(old.keys()) | set(new.keys()):
      if old.get(key) != new.get(key):
        if key == "memory" and validated:
          pass
        elif key in ("time_event", "size_event") and not validated:
          pass
        elif key in ("total_events", "generators", "tags", "memory"):
          different.add(key)
        elif key == "sequences":
          assert len(old[key]) == len(new[key]) == 1
          for skey in set(old[key][0].keys()) | set(new[key][0].keys()):
            if old[key][0].get(skey) != new[key][0].get(skey):
              if skey == "nThreads":
                differentsequences.add(skey)
              elif skey == "procModifiers" and old[key][0].get(skey) is None and new[key][0].get(skey) == "":
                pass  #not sure what this is about
              else:
                raise ValueError("Don't know what to do with {} ({} --> {}) in sequences for {}".format(skey, old[key][0].get(skey), new[key][0].get(skey), self.prepid))
        else:
          raise ValueError("Don't know what to do with {} ({} --> {}) for {}".format(key, old.get(key), new.get(key), self.prepid))
    if different or differentsequences:
      self.needsupdate = True
      return "there is a change in some parameters, setting needsupdate = True:\n" + "\n".join("{}: {} --> {}".format(*_) for _ in itertools.chain(((key, old.get(key), new.get(key)) for key in different), ((skey, old["sequences"][0].get(skey), new["sequences"][0].get(skey)) for skey in differentsequences)))


class MCSampleBase_DefaultCampaign(MCSampleBase):
  @property
  def campaign(self):
    if self.year == 2016:
      return "RunIISummer15wmLHEGS"
    if self.year == 2017:
      return "RunIIFall17wmLHEGS"
    if self.year == 2018:
      return "RunIIFall18wmLHEGS"
    assert False, self.year
