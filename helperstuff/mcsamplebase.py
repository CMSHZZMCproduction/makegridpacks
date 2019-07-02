import abc, collections, contextlib, filecmp, glob, itertools, json, os, pprint, pycurl, re, shutil, stat, subprocess, tempfile

import uncertainties

import patches

from utilities import cache, cacheaslist, cd, cdtemp, cmsswversion, createLHEProducer, fullinfo, genproductions, here, jobended, JsonDict, KeepWhileOpenFile, mkdir_p, osversion, request_fragment_check, scramarch, urlopen, wget
from jobsubmission import condortemplate_filter, condortemplate_sizeperevent, jobid, JobQueue, jobtype, queuematches, submitcondor
from rest import McM

class MCSampleBase(JsonDict):
  @abc.abstractmethod
  def __init__(self, year):
    self.__year = int(year)
  @abc.abstractproperty
  def initargs(self): assert False, self
  @property
  def initkwargs(self): return {}
  @property
  def year(self):
    return self.__year
  @abc.abstractproperty
  def identifiers(self):
    """example: productionmode, decaymode, mass"""
    assert False, self
  @abc.abstractproperty
  def tarballversion(self): assert False, self
  @property
  def cvmfstarball(self):
    result = self.cvmfstarball_anyversion(self.tarballversion)
    if self.uselocaltarballfortest:
      if self.dovalidation: raise ValueError("{} uses a local tarball for testing purposes, so you have to set dovalidation to false".format(self))
      result = result.replace("/cvmfs/cms.cern.ch/phys_generator/", here+"/test_")
    return result
  @property
  def uselocaltarballfortest(self):
    return False
  @abc.abstractmethod
  def cvmfstarball_anyversion(self, version): assert False, self
  @abc.abstractproperty
  def tmptarballbasename(self): assert False, self
  @abc.abstractproperty
  def makegridpackcommand(self): assert False, self
  @abc.abstractproperty
  def makinggridpacksubmitsjob(self):
    """returns the job name"""
    assert False, self
  @abc.abstractproperty
  def hasfilter(self): return False
  @abc.abstractproperty
  def datasetname(self): assert False, self
  @property
  def extensionnumber(self):
    """This should normally be 0, only change it for extension samples"""
    return 0
  @abc.abstractproperty
  def nevents(self): assert False, self
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
  @cache
  def getcardsurl(self):
    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      self.__calledsupercardsurl = False
      result = self.cardsurl
      if not self.__calledsupercardsurl:
        raise TypeError("super of cardsurl for {} didn't propagate all the way up to MCSampleBase".format(self))
      return result
  @abc.abstractproperty
  def cardsurl(self):
    """
    runs in a tmpdir where the gridpack has been opened.
    You can and should do all kinds of checks here.
    At the end it returns the urls of the input cards.
    """
    self.__calledsupercardsurl = True
    for root, dirnames, filenames in os.walk('.'):
      for filename in filenames:
        if re.match("core[.].*", filename):
          raise ValueError("There is a core dump in the tarball\n{}".format(self))
    result = ""
    if self.uselocaltarballfortest:
      result += "\n# this is here to fool the test script: /cvmfs/cms.cern.ch/phys_generator/gridpacks"
    return result.lstrip("\n")
  @abc.abstractproperty
  def defaulttimeperevent(self): assert False, self
  @abc.abstractproperty
  def tags(self): assert False, self
  @abc.abstractproperty
  def fragmentname(self): assert False, self
  @abc.abstractproperty
  def genproductionscommit(self): assert False, self
  @property
  def genproductionscommitforfragment(self): return self.genproductionscommit
  @abc.abstractproperty
  def makegridpackscriptstolink(self): assert False, self
  @abc.abstractproperty
  def xsec(self): assert False, self
  @abc.abstractproperty
  def responsible(self): "put the lxplus username of whoever makes these gridpacks"
  @property
  def patchkwargs(self): return []
  @property
  def validationtimemultiplier(self): return 1
  @property
  def neventsfortest(self): return None
  @property
  def notes(self): return ""
  @property
  def creategridpackqueue(self): return "tomorrow"
  @property
  def timepereventqueue(self): return "tomorrow"
  @property
  def timepereventflavor(self): return JobQueue(self.timepereventqueue).condorflavor
  @property
  def filterefficiencyqueue(self): return "tomorrow"
  @property
  def filterefficiencyflavor(self): return JobQueue(self.filterefficiencyqueue).condorflavor
  @property
  def dovalidation(self):
    """Set this to false if a request fails so badly that the validation will never succeed"""
    return True
  @property
  def holdrequest(self):
    """set this to true if you don't want to start the size and time etc. quite yet"""
    return False
  @property
  def inthemiddleofmultistepgridpackcreation(self):
    """powheg samples that need to run the grid in multiple steps should sometimes return true"""
    return False
  @property
  def gridpackjobsrunning(self):
    """powheg samples that need to run the grid in multiple steps need to modify this"""
    if not self.makinggridpacksubmitsjob: return False
    raise RuntimeError("fix this")
    #this doesn't work
    return not jobended("-constraint", 'JobBatchName == "{}"'.format(self.makinggridpacksubmitsjob))
  def processmakegridpackstdout(self, stdout): "do nothing by default, powheg uses this"

  @abc.abstractmethod
  @cacheaslist
  def allsamples(self): "should be a classmethod"; assert False, self

  def __eq__(self, other):
    return self.keys == other.keys
  def __ne__(self, other):
    return not (self == other)
  def __hash__(self):
    return hash(self.keys)
  def __str__(self):
    return " ".join(str(_) for _ in self.keys)
  def __repr__(self):
    return (type(self).__name__+"(" +  ", ".join(repr(_) for _ in self.initargs) + 
                                       ", ".join("{}={!r}".format(k, v) for k, v in self.initkwargs.iteritems()) + ")")

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
      self.getcardsurl()
    except Exception as e:
      if str(self) in str(e):
        return str(e).replace(str(self), "").strip()
      else:
        raise

  @property
  def workdirforgridpack(self):
    result = os.path.dirname(self.foreostarball)
    result = result.replace(os.path.join(here, "gridpacks"), os.path.join(here, "workdir"))
    assert result.startswith(os.path.join(here, "workdir")), result
    return result

  @property
  def workdir(self):
    return self.workdirforgridpack.rstrip("/") + "_" + str(self).replace(" ", "")

  @property
  def tmptarball(self):
    return os.path.join(self.workdirforgridpack, self.tmptarballbasename)

  @property
  def cvmfstarballexists(self): return os.path.exists(self.cvmfstarball)

  def patchtarball(self):
    if os.path.exists(self.cvmfstarball) or os.path.exists(self.eostarball) or os.path.exists(self.foreostarball): return
    from . import allsamples

    if not self.needspatch: assert False

    samples = list(allsamples(lambda x: hasattr(x, "cvmfstarball") and x.cvmfstarball == self.cvmfstarball))

    mkdir_p(os.path.dirname(self.tmptarball))
    with KeepWhileOpenFile(self.tmptarball+".tmp") as kwof:
      if not kwof:
        return "job to patch the tarball is already running"

      kwargs = {
        json.dumps(_.needspatch) for _ in samples if _.needspatch
      }
      if len(kwargs) != 1:
        raise ValueError("Different samples, which all use {}, have different needspatch kwargs:\n{}".format(self.cvmfstarball, "\n".join(str(_) for _ in kwargs)))

      kwargs = json.loads(kwargs.pop())
      if isinstance(kwargs, int):
        kwargs = self.patchkwargs
        if isinstance(kwargs, list):
          kwargs = {"functionname": "multiplepatches", "listofkwargs": kwargs}
        kwargs["oldtarballversion"] = self.needspatch
      if "oldfilename" in kwargs or "newfilename" in kwargs or "sample" in kwargs: assert False, kwargs
      kwargs["oldfilename"] = self.cvmfstarball_anyversion(version=kwargs.pop("oldtarballversion"))
      kwargs["newfilename"] = self.foreostarball
      mkdir_p(os.path.dirname(self.foreostarball))

      patches.dopatch(**kwargs)

      if not os.path.exists(self.foreostarball): raise RuntimeError("Patching failed, gridpack doesn't exist")
      for _ in samples:
        if _.timeperevent is not None:
          del _.timeperevent
        _.needspatch = False

      return "tarball is patched and the new version is in this directory to be copied to eos"

  def createtarball(self):
    if os.path.exists(self.cvmfstarball) or os.path.exists(self.eostarball) or os.path.exists(self.foreostarball): return
    from . import allsamples

    mkdir_p(os.path.dirname(self.tmptarball))
    with cd(self.workdirforgridpack), KeepWhileOpenFile(self.tmptarball+".tmp") as kwof:
      try:
        with open(self.tmptarball+".jobid") as f:
          condorjobid = f.read().strip()
      except IOError:
        pass
      else:
        if condorjobid != jobid():
          if jobended(condorjobid):
            os.remove(self.tmptarball+".jobid")
          else:
            return "condor job "+condorjobid+" is already taking care of this tarball"

      if not kwof:
        try:
          with open(self.tmptarball+".tmp") as f:
            kwofjobid = float(f.read().strip())
        except (ValueError, IOError):
          return "try running again, probably you just got really bad timing"
        if jobended(str(kwofjobid)):
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
          if not jobtype(): return "need to create the gridpack, submitting to condor" if submitcondor(self.creategridpackqueue, sample=self, writejobid=self.tmptarball+".jobid") else "need to create the gridpack, job is pending on condor"
          if not queuematches(self.creategridpackqueue): return "need to create the gridpack, but on the wrong queue"
        for filename in self.makegridpackscriptstolink:
          os.symlink(filename, os.path.basename(filename))

        makinggridpacksubmitsjob = self.makinggridpacksubmitsjob

        #https://stackoverflow.com/a/17698359/5228524
        makegridpackstdout = ""
        pipe = subprocess.Popen(self.makegridpackcommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
        for line in pipe.stdout:
            print line,
            makegridpackstdout += line
        self.processmakegridpackstdout(makegridpackstdout)
        pipe.communicate()
        if pipe.returncode:
            raise RuntimeError("makegridpackcommand {} gave an error ^^^".format(self.makegridpackcommand))

        if makinggridpacksubmitsjob:
          return "submitted the gridpack creation job"
        if self.inthemiddleofmultistepgridpackcreation:
          return "ran one step of gridpack creation, run again to continue"

      mkdir_p(os.path.dirname(self.foreostarball))
      if self.patchkwargs:
        kwargs = self.patchkwargs
        if isinstance(kwargs, list):
          kwargs = {"functionname": "multiplepatches", "listofkwargs": kwargs}
        for _ in "oldfilename", "newfilename", "sample": assert _ not in kwargs, _
        with cdtemp():
          kwargs["oldfilename"] = self.tmptarball
          kwargs["newfilename"] = os.path.abspath(os.path.basename(self.tmptarball))
          #kwargs["sample"] = self #???
          patches.dopatch(**kwargs)
          shutil.move(os.path.basename(self.tmptarball), self.tmptarball)

      for _ in allsamples(lambda x: hasattr(x, "cvmfstarball") and x.cvmfstarball == self.cvmfstarball):
        if _.timeperevent is not None:
          del _.timeperevent
      shutil.move(self.tmptarball, self.foreostarball)
      shutil.rmtree(os.path.dirname(self.tmptarball))
      return "tarball is created and moved to this folder, to be copied to eos"

  def findfilterefficiencycondor(self):
    #figure out the filter efficiency
    if not self.hasfilter:
      self.filterefficiency = 1
      return "filter efficiency is set to 1 +/- 0"

    if not self.implementsfilter: raise ValueError("Can't find filter efficiency for {.__name__} which doesn't implement filtering!".format(type(self)))
    mkdir_p(self.workdir)
    jobsrunning = False
    eventsprocessed = eventsaccepted = nfinished = 0
    jobstosubmit = []
    with cd(self.workdir):
      for i in range(100):
        mkdir_p(str(i))
        with cd(str(i)), KeepWhileOpenFile("runningfilterjob.tmp", deleteifjobdied=True) as kwof:
          if not kwof:
            jobsrunning = True
            continue
          if os.path.exists("filterjob.log"):
            with open("filterjob.log") as f:
              for line in f:
                if line.startswith("004") or line.startswith("005") or line.startswith("009"): break
              else:
                jobsrunning = True
                continue
          if os.path.exists(self.filterresultsfile):
            processed, accepted = self.getfilterresults(i)
            if not processed is accepted is None:
              eventsprocessed += processed
              eventsaccepted += accepted
              nfinished += 1
              continue
          if not os.path.exists(self.filterresultsfile):
            with open("filterjobscript", "w") as f:
              f.write(self.filterjobscript(i))
            try:
              os.remove("filterjob.log")
            except OSError:
              pass
            os.chmod("filterjobscript", os.stat("filterjobscript").st_mode | stat.S_IEXEC)
            jobstosubmit.append(str(i))
            continue

      if jobstosubmit:
        jobsrunning = True
        with tempfile.NamedTemporaryFile(bufsize=0) as f:
          f.write(condortemplate_filter(self=self, jobstoqueue=" ".join(jobstosubmit)))
          subprocess.check_call(["condor_submit", f.name])
        return "submitted {} filter efficiency jobs".format(len(jobstosubmit))

      if jobsrunning: return "some filter efficiency jobs are still running"
      assert nfinished == 100, nfinished
      self.filterefficiency = uncertainties.ufloat(1.0*eventsaccepted / eventsprocessed, (1.0*eventsaccepted * (eventsprocessed-eventsaccepted) / eventsprocessed**3) ** .5)
      #shutil.rmtree(self.workdir)
      return "filter efficiency is measured to be {}".format(self.filterefficiency)

  implementsfilter = False

  def getsizeandtimecondor(self):
    mkdir_p(self.workdir)
    xmlfile = self.prepid+"_rt.xml"
    with KeepWhileOpenFile(os.path.join(self.workdir, self.prepid+".tmp"), deleteifjobdied=True) as kwof:
      if not kwof: return "job to get the size and time is already running"
      with cd(self.workdir):
        if not os.path.exists(xmlfile) or os.stat(xmlfile).st_size == 0:
          for jobfile in glob.glob("*.log"):
            with open(jobfile) as f:
              for line in f:
                if line.startswith("004") or line.startswith("005") or line.startswith("009"): break
              else:
                return "job {} to get the size and time is already running".format(jobfile.replace(".log", ""))
          if jobtype(): return "need to get time and size per event, run locally to submit to condor"
          check = self.request_fragment_check()
          if check: return check
          wget(os.path.join("https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_test/", self.prepid, str(self.neventsfortest) if self.neventsfortest else "").rstrip("/"), output=self.prepid)
          with open(self.prepid) as f:
            testjob = f.read()
            try:
              testjob = eval(testjob)  #sometimes it's a string
            except SyntaxError:
              pass                     #sometimes it's not
          if self.tweaktimepereventseed:
            lines = testjob.split("\n")
            cmsdriverindex = {i for i, line in enumerate(lines) if "cmsDriver.py" in line}
            assert len(cmsdriverindex) == 1, cmsdriverindex
            cmsdriverindex = cmsdriverindex.pop()
            lines.insert(cmsdriverindex+1, 'sed -i "/Services/aprocess.RandomNumberGeneratorService.externalLHEProducer.initialSeed = process.RandomNumberGeneratorService.externalLHEProducer.initialSeed.value() + {:d}" *_cfg.py'.format(self.tweaktimepereventseed))
            testjob = "\n".join(lines)

          for scramarchversion in re.findall("slc([0-9]+)", testjob):
            if int(scramarchversion) != osversion:
              raise ValueError("Wrong slc version {} in {}".format(scramarchversion, os.path.abspath(self.prepid)))

          with open(self.prepid, "w") as newf:
            newf.write(testjob)
          os.chmod(self.prepid, os.stat(self.prepid).st_mode | stat.S_IEXEC)
          with tempfile.NamedTemporaryFile(bufsize=0) as f:
            f.write(condortemplate_sizeperevent(self=self))
            subprocess.check_call(["condor_submit", f.name])
          return "need to get time and size per event, submitting to condor"

        with open(xmlfile) as f:
          nevents = totalsize = None
          for line in f:
            if "<FrameworkError" in line:
              abspath = os.path.abspath(xmlfile)
              with cd(here):
                return "Job to get the size and time per event failed, please check "+os.path.relpath(abspath)+" and the condor output files in that directory"
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

      if not (self.sizeperevent and self.timeperevent):
        return "failed to get the size and time"

      shutil.rmtree(self.workdir)

      if jobtype(): return "size and time per event are found to be {} and {}, run locally to send to McM".format(self.sizeperevent, self.timeperevent)
      self.updaterequest()
      return "size and time per event are found to be {} and {}, sent it to McM".format(self.sizeperevent, self.timeperevent)

  def getsizeandtime(self):
    mkdir_p(self.workdir)
    with KeepWhileOpenFile(os.path.join(self.workdir, self.prepid+".tmp"), deleteifjobdied=True) as kwof:
      if not kwof: return "job to get the size and time is already running"
      if not jobtype(): return "need to get time and size per event, submitting to LSF" if submitLSF(self.timepereventqueue) else "need to get time and size per event, job is pending on LSF"
      if not queuematches(self.timepereventqueue): return "need to get time and size per event, but on the wrong queue"
      with cdtemp():
        wget(os.path.join("https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_test/", self.prepid, str(self.neventsfortest) if self.neventsfortest else "").rstrip("/"), output=self.prepid)
        with open(self.prepid) as f:
          testjob = f.read()
          try:
            testjob = eval(testjob)  #sometimes it's a string
          except SyntaxError:
            pass                     #sometimes it's not
        with open(self.prepid, "w") as newf:
          newf.write(testjob)
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
    if jobtype(): return "size and time per event are found to be {} and {}, run locally to send to McM".format(self.sizeperevent, self.timeperevent)
    self.updaterequest()
    return "size and time per event are found to be {} and {}, sent it to McM".format(self.sizeperevent, self.timeperevent)

  def makegridpack(self, approvalqueue, badrequestqueue, clonequeue, setneedsupdate=False):
    if self.finished: return "finished!"
    if not self.cvmfstarballexists:
      if not os.path.exists(self.eostarball):
        if self.cmsswversion != cmsswversion or self.scramarch != scramarch: return "try again in "+self.cmsswversion+" with scram arch "+self.scramarch
        if not os.path.exists(self.foreostarball):
          if self.needspatch: return self.patchtarball()
          return self.createtarball()
        return "gridpack exists in this folder, to be copied to eos" 
      return "gridpack exists on eos, not yet copied to cvmfs"

    if os.path.exists(self.foreostarball):
      if filecmp.cmp(self.cvmfstarball, self.foreostarball, shallow=False):
        if self.cvmfstarball.startswith("/cvmfs/"):
          from . import allsamples
          for _ in allsamples(lambda x: hasattr(x, "cvmfstarball") and x.cvmfstarball == self.cvmfstarball):
            _.needsupdate = True
          os.remove(self.foreostarball)
      else:
        return "gridpack exists on cvmfs, but it's wrong!"

    if not self.makerequest:
      self.finished = True
      return "finished!"

    if self.badprepid:
      badrequestqueue.add(self)

    if self.prepid is None:
      self.getprepid()
      if self.prepid is None:
        #need to make the request
        return self.createrequest(clonequeue)
      else:
        return "found prepid: {}".format(self.prepid)

    if (self.approval, self.status) == ("submit", "done"):
      if self.needsupdate:
        return "{} is already finished, but needs update!".format(self)
      self.needsupdateiffailed = False
      self.gettimepereventfromMcM()
      self.finished = True
      return "finished!"

    if self.holdrequest and not self.needsupdateiffailed:
      return "holding request, set holdrequest to false and run again to continue"

    if self.filterefficiency is None and not self.needsupdateiffailed:
      if os.path.exists(self.cvmfstarball_anyversion(self.tarballversion+1)): self.needsupdate=True; return "tarball version is v{}, but v{} exists".format(self.tarballversion, self.tarballversion+1)
      if setneedsupdate:
        result = self.setneedsupdate()
        if result: return result
      if self.cmsswversion != cmsswversion or self.scramarch != scramarch: return "try again in "+self.cmsswversion+" with scram arch "+self.scramarch
      return self.findfilterefficiencycondor()

    if not (self.sizeperevent and self.timeperevent) and not self.needsupdateiffailed:
      if os.path.exists(self.cvmfstarball_anyversion(self.tarballversion+1)): self.needsupdate=True; return "tarball version is v{}, but v{} exists".format(self.tarballversion, self.tarballversion+1)
      if setneedsupdate:
        result = self.setneedsupdate()
        if result: return result
      if self.cmsswversion != cmsswversion or self.scramarch != scramarch: return "try again in "+self.cmsswversion+" with scram arch "+self.scramarch
      return self.getsizeandtimecondor()

    if jobtype():
      return "please run locally to check and/or advance the status".format(self.prepid)

    if self.badprepid:
      badrequestqueue.add(self)

    if (self.approval, self.status) == ("none", "new"):
      if os.path.exists(self.cvmfstarball_anyversion(self.tarballversion+1)): self.needsupdate=True; return "tarball version is v{}, but v{} exists".format(self.tarballversion, self.tarballversion+1)
      if self.needsoptionreset:
        if not self.optionreset():
          return "need to do option reset but failed"
        return "needed option reset, sent it to McM"
      if self.needsupdateiffailed:
        self.updaterequest()
        if self.badprepid:
          badrequestqueue.add(self)
        return "needs update on McM, sending it there"
      if not self.dovalidation:
        if setneedsupdate and not self.needsupdate:
          result = self.setneedsupdate()
          if result: return result
        return "not starting the validation"
      if self.nthreads > 1 and self.fullinfo["history"][-1]["action"] == "failed" and self.fullinfo["validation"]["validations_count"] == 1:
        self.nthreads /= 2
        self.updaterequest()
        return "validation failed, decreasing the number of threads"
      if setneedsupdate and not self.needsupdate:
        result = self.setneedsupdate()
        if result: return result
      check = self.request_fragment_check()
      if check: return check
      approvalqueue.validate(self)
      return "starting the validation"
    if (self.approval, self.status) == ("validation", "new"):
      if setneedsupdate and not self.needsupdate:
        result = self.setneedsupdate()
        if result: return result
      return "validation is running"
    if (self.approval, self.status) == ("validation", "validation"):
      if os.path.exists(self.cvmfstarball_anyversion(self.tarballversion+1)): return "tarball version is v{}, but v{} exists".format(self.tarballversion, self.tarballversion+1)
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
      try:
        del self.value["prepid"]
      except KeyError:
        pass
  @property
  def otherprepids(self):
    """
    This is meant to store prepids that were automatically cloned from this prepid to use in a new chain.
    You don't have to set otherprepids unless you want to use them e.g. for RedoForceCompletedSample.
    """
    with cd(here):
      return self.value.get("otherprepids", [])
  def addotherprepid(self, otherprepid):
    if otherprepid == self.prepid: return
    if otherprepid in self.otherprepids: return
    history = fullinfo(otherprepid)["history"]
    clone = history[1]
    if not (
      clone["action"] == "clone" and
      clone["step"] in [self.prepid] + self.otherprepids and
      clone["updater"]["author_username"] == "pdmvserv"
    ):
      raise ValueError(otherprepid + " was not cloned from " + str(self) + "("+str(self.prepid)+") by pdmvserv\n" + json.dumps(clone))
    for action in history:
      if (
        action["action"] == "update" and
        action["updater"]["author_username"] not in ("pdmvserv", "automatic")
      ):
        raise ValueError(otherprepid + " was updated by someone besides pdmvserv and automatic\n" + json.dumps(action))

    with cd(here), self.writingdict():
      if not self.otherprepids:
        self.value["otherprepids"] = []
      self.otherprepids.append(otherprepid)
  @otherprepids.deleter
  def otherprepids(self):
    with cd(here), self.writingdict():
      try:
        del self.value["otherprepids"]
      except KeyError:
        pass
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
      try:
        del self.value["timeperevent"]
      except KeyError:
        pass
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
      try:
        del self.value["resettimeperevent"]
      except KeyError:
        pass
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
      try:
        del self.value["sizeperevent"]
      except KeyError:
        pass
  @property
  def filterefficiency(self):
    if self.filterefficiencynominal is None or self.filterefficiencyerror is None: return None
    return uncertainties.ufloat(self.filterefficiencynominal, self.filterefficiencyerror)
  @filterefficiency.setter
  def filterefficiency(self, value):
    nominal, error = uncertainties.nominal_value(value), uncertainties.std_dev(value)
    if error == 0 and nominal != 1: raise ValueError("Are you sure you want to set the filterefficiency to {} with no error?".format(uncertainties.ufloat(nominal, error)))
    self.filterefficiencynominal = nominal
    self.filterefficiencyerror = error
  @filterefficiency.deleter
  def filterefficiency(self):
    del self.filterefficiencynominal, self.filterefficiencyerror
  @property
  def filterefficiencynominal(self):
    with cd(here):
      return self.value.get("filterefficiency")
  @filterefficiencynominal.setter
  def filterefficiencynominal(self, value):
    with cd(here), self.writingdict():
      self.value["filterefficiency"] = value
    self.needsupdate = True
  @filterefficiencynominal.deleter
  def filterefficiencynominal(self):
    with cd(here), self.writingdict():
      try:
        del self.value["filterefficiency"]
      except KeyError:
        pass
  @property
  def filterefficiencyerror(self):
    with cd(here):
      return self.value.get("filterefficiencyerror")
  @filterefficiencyerror.setter
  def filterefficiencyerror(self, value):
    with cd(here), self.writingdict():
      self.value["filterefficiencyerror"] = value
    self.needsupdate = True
  @filterefficiencyerror.deleter
  def filterefficiencyerror(self):
    with cd(here), self.writingdict():
      try:
        del self.value["filterefficiencyerror"]
      except KeyError:
        pass
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
      try:
        del self.value["needsupdate"]
      except KeyError:
        pass
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
      try:
        del self.value["needsupdateiffailed"]
      except KeyError:
        pass
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
      try:
        del self.value["needsoptionreset"]
      except KeyError:
        pass
  @property
  def badprepid(self):
    with cd(here):
      result = self.value.get("badprepid", [])
      #backwards compatibility
      if isinstance(result, basestring): result = [result]

      originalresult = result[:]
      for _ in result[:]:
        if not jobtype() and not McM().get("requests", _):
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
      try:
        del self.value["badprepid"]
      except KeyError:
        pass
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
      try:
        del self.value["finished"]
      except KeyError:
        pass
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
      try:
        del self.value["needspatch"]
      except KeyError:
        pass
  @property
  def nthreads(self):
    with cd(here):
      if "nthreads" not in self.value and self.prepid and (self.finished or self.status in ("submitted", "approved")):
        self.value["nthreads"] = self.fullinfo["sequences"][0]["nThreads"]
      return self.value.get("nthreads", self.defaultnthreads)
  @abc.abstractproperty
  def defaultnthreads(self): pass
  @nthreads.setter
  def nthreads(self, value):
    if "nthreads" in self.value and value == self.nthreads: return
    with cd(here), self.writingdict():
      self.value["nthreads"] = int(value)
    if self.timeperevent:
      del self.timeperevent
  @nthreads.deleter
  def nthreads(self):
    with cd(here), self.writingdict():
      try:
        del self.value["nthreads"]
      except KeyError:
        pass

  @property
  def memory(self):
    if self.nthreads == 1: return 2300
    return 4000
  @property
  def matchefficiency(self): return uncertainties.ufloat(1, 0)

  @property
  @cache
  def fullfragment(self):
    return createLHEProducer(self.cvmfstarball, self.getcardsurl(), self.fragmentname, self.genproductionscommitforfragment)

  @property
  def tune(self):
    return {
      "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_generic_LHE_pythia8_cff.py": "TuneCP5",
    }[self.fragmentname]

  def getdictforupdate(self):
    mcm = McM()
    req = mcm.get("requests", self.prepid)
    try:
      req["dataset_name"] = self.datasetname
      req["mcdb_id"] = 0
      req["total_events"] = self.nevents
      req["fragment"] = self.fullfragment
      req["time_event"] = [(self.timeperevent if self.timeperevent is not None else self.defaulttimeperevent)]
      req["size_event"] = [self.sizeperevent if self.sizeperevent is not None else 600]
      req["generators"] = self.generators
      if self.filterefficiency is not None:
        if not req["generator_parameters"]: req["generator_parameters"].append({})
        req["generator_parameters"][0].update({
          "filter_efficiency": self.filterefficiency.nominal_value,
          "filter_efficiency_error": self.filterefficiency.std_dev,
          "match_efficiency": self.matchefficiency.nominal_value,
          "match_efficiency_error": self.matchefficiency.std_dev,
          "cross_section": uncertainties.nominal_value(self.xsec),
        })
      req["sequences"][0]["nThreads"] = self.nthreads
      req["keep_output"][0] = bool(self.keepoutput)
      req["tags"] = list(self.tags)
      req["memory"] = self.memory
      req["validation"].update({
        "time_multiplier": self.validationtimemultiplier,
      })
      req["extension"] = self.extensionnumber
      req["notes"] = self.notes
    except:
      pprint.pprint(req)
      raise
    return req

  def updaterequest(self):
    mcm = McM()
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
  def campaign(self): assert False, self

  def createrequest(self, clonequeue):
    if jobtype(): return "run locally to submit to McM"
    mcm = McM()
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
    if jobtype(): return
    query = "dataset_name={}&extension={}&prepid={}-{}-*".format(self.datasetname, self.extensionnumber, self.pwg, self.campaign)
    output = McM().get('requests', query=query)
    if not output:
      return None
    prepids = {_["prepid"] for _ in output}
    prepids -= frozenset(self.badprepid)
    if not prepids:
      return None
    if len(prepids) != 1:
      raise RuntimeError("Multiple prepids for {} ({} ext {})\n{}".format(self, self.datasetname, self.extensionnumber, query))
    assert len(prepids) == 1, prepids
    self.prepid = prepids.pop()

  @property
  def fullinfo(self):
    if not self.prepid: raise AttributeError("Can only call fullinfo once the prepid has been set")
    return fullinfo(self.prepid)

  def otherfullinfo(self, i):
    if i==0: return self.fullinfo
    return self.otherprepids[i-1].fullinfo

  def gettimepereventfromMcM(self):
    if (self.timeperevent is None or self.resettimeperevent) and not (self.prepid and self.status in ("approved", "submitted", "done")) and not (self.status in ("validation", "defined") and not self.needsupdate): return
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
    if not self.prepid: return "none"
    return self.fullinfo["approval"]
  @property
  def status(self):
    if self.finished: return "done"
    if not self.prepid: return "new"
    return self.fullinfo["status"]

  def delete(self):
    response = ""
    while response not in ("yes", "no"):
      response = raw_input("are you sure you want to delete {}? [yes/no]".format(self))
    if response == "no": return
    if self.prepid:
      McM().approve("requests", self.prepid, 0)
      McM().delete("requests", self.prepid)
    with cd(here), self.writingdict():
      del self.value

  def optionreset(self):
    if self.prepid is None: return
    self.needsupdate = True
    results = McM().get("restapi/requests/option_reset/"+self.prepid)
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
    differentgenparameters = set()
    setneedsupdate = False
    setneedsupdateiffailed = False
    differentsub = collections.defaultdict(set)
    differentsublist = collections.defaultdict(set)
    for key in set(old.keys()) | set(new.keys()):
      if old.get(key) != new.get(key):
        if key == "memory" and validated:
          pass
        elif key in ("time_event", "size_event") and not validated:
          pass
        elif key in ("total_events", "generators", "tags", "dataset_name", "fragment", "notes", "keep_output"):
          different.add(key)
          setneedsupdate = True
        elif key in ("memory",):
          different.add(key)
          setneedsupdateiffailed = True
        elif key == "sequences":
          assert len(old[key]) == len(new[key]) == 1
          for subkey in set(old[key][0].keys()) | set(new[key][0].keys()):
            if old[key][0].get(subkey) != new[key][0].get(subkey):
              if subkey == "nThreads":
                setneedsupdateiffailed = True
                differentsublist["sequences"].add(subkey)
              elif subkey == "procModifiers" and old[key][0].get(subkey) is None and new[key][0].get(subkey) == "":
                pass  #not sure what this is about
              else:
                raise ValueError("Don't know what to do with {} ({} --> {}) in sequences for {}".format(subkey, old[key][0].get(subkey), new[key][0].get(subkey), self.prepid))
        elif key == "generator_parameters":
          assert len(old[key]) == len(new[key]) == 1
          for subkey in set(old[key][0].keys()) | set(new[key][0].keys()):
            if old[key][0].get(subkey) != new[key][0].get(subkey):
              if subkey in ("filter_efficiency", "filter_efficiency_error", "match_efficiency", "match_efficiency_error", "cross_section"):
                setneedsupdate = True
                differentsublist["generator_parameters"].add(subkey)
              else:
                raise ValueError("Don't know what to do with {} ({} --> {}) in generator_parameters for {}".format(subkey, old[key][0].get(subkey), new[key][0].get(subkey), self.prepid))
        elif key == "validation":
          for subkey in set(old[key].keys()) | set(new[key].keys()):
            if old[key].get(subkey) != new[key].get(subkey):
              if subkey in ("double_time", "time_multiplier"):
                setneedsupdateiffailed = True
                differentsub["validation"].add(subkey)
              else:
                raise ValueError("Don't know what to do with {} ({} --> {}) in validation for {}".format(subkey, old[key][0].get(subkey), new[key][0].get(subkey), self.prepid))
        else:
          raise ValueError("Don't know what to do with {} ({} --> {}) for {}".format(key, old.get(key), new.get(key), self.prepid))
    if setneedsupdate or setneedsupdateiffailed:
      if setneedsupdate:
        self.needsupdate = True
      else:
        self.needsupdateiffailed = True
      return "there is a change in some parameters, setting needsupdate" + "iffailed"*(not setneedsupdate) + " = True:\n" + "\n".join("{}: {} --> {}".format(*_) for _ in itertools.chain(((key, old.get(key), new.get(key)) for key in different), ((subkey, old[key].get(subkey), new[key].get(subkey)) for key in differentsub for subkey in differentsub[key]), ((subkey, old[key][0].get(subkey), new[key][0].get(subkey)) for key in differentsub for subkey in differentsublist[key])))

  def request_fragment_check(self):
    if self.fullinfo["cmssw_release"].endswith("X"):
      return "CMSSW release is "+self.fullinfo["cmssw_release"]+", which is not a real release.  Once the campaign has a real release, do an option reset."

    try:
      output = request_fragment_check("--prepid", self.prepid, "--bypass_status")
      print output,
    except subprocess.CalledProcessError as e:
      print e.output,
      return "request_fragment_check failed!"

    for line in output.split("\n"):
      if line == self.prepid: continue
      if re.match(r"{}\s*Status\s*=\s*\w*".format(self.prepid), line.strip()): continue
      elif "cookie" in line: continue
      elif not line.strip().strip("*"): continue
      elif "will be checked:" in line: continue
      elif line.startswith("* [OK]"): continue
      elif line.startswith("* [ERROR]"): return "request_fragment_check gave an error!\n"+line
      elif line.startswith("* [WARNING]"):
        result = self.handle_request_fragment_check_warning(line)
        if result == "ok": continue
        return result+"\n"+line
      elif line.startswith("* [Caution: To check manually]"):
        result = self.handle_request_fragment_check_caution(line)
        if result == "ok": continue
        return result+"\n"+line
      elif line.startswith("* [PATCH]"):
        result = self.handle_request_fragment_check_patch(line)
        if result == "ok": continue
        return result+"\n"+line
      elif line.startswith("* ["):
        return "Unknown line starting with * [ in request_fragment_check output!\n"+line

  @property
  def maxallowedtimeperevent(self): return None  #default to whatever request_fragment_check does

  def handle_request_fragment_check_warning(self, line):
    if re.match(r"\* \[WARNING\] Large time/event[=0-9.]* - please check", line.strip()):
      print "time/event is", self.fullinfo["time_event"][0]
      if self.maxallowedtimeperevent is not None and self.timeperevent < self.maxallowedtimeperevent: return "ok"
      return "please check it"
    if line == "* [WARNING] Do you really want to have tune CP5 in this campaign?":
      if self.year == 2016 and "/2017/" in self.cvmfstarball:
        return "ok"
    return "request_fragment_check gave an unhandled warning!"

  def handle_request_fragment_check_caution(self, line):
    return "request_fragment_check gave an unhandled caution!"

  def handle_request_fragment_check_patch(self, line):
    return "request_fragment_check gave an unhandled patch!"

  @property
  def tweaktimepereventseed(self):
    return None
  @property
  def tweakmakegridpackseed(self):
    return 0

  @property
  def makerequest(self):
    return True

  @abc.abstractmethod
  def findPDFfromtarball(self): pass

  @abc.abstractproperty
  def cmsswversion(self):
    return "CMSSW_9_3_0"
  @abc.abstractproperty
  def scramarch(self):
    return "slc6_amd64_gcc630"

class Run2MCSampleBase(MCSampleBase):
  @property
  def campaign(self):
    if self.year == 2016:
      return "RunIISummer15wmLHEGS"
    if self.year == 2017:
      return "RunIIFall17wmLHEGS"
    if self.year == 2018:
      return "RunIIFall18wmLHEGS"
    assert False, self.year
  @property
  def cmsswversion(self):
    return "CMSSW_9_3_0"
  @property
  def scramarch(self):
    return "slc6_amd64_gcc630"
  @property
  def defaultnthreads(self): return 8 if self.year >= 2017 else 1


class Run2UltraLegacyBase(MCSampleBase):
  @property
  def campaign(self):
    if self.year == 2016:
      return "RunIISummer19UL16wmLHEGEN"
    if self.year == 2017:
      return "RunIISummer19UL17wmLHEGEN"
    if self.year == 2018:
      return "RunIISummer19UL18wmLHEGEN"
    assert False, self.year
  @property
  def cmsswversion(self):
    return "CMSSW_9_3_14"
  @property
  def scramarch(self):
    return "slc7_amd64_gcc630"
  @property
  def keepoutput(self): return True



  @property
  def cardsurl(self):
    PDFname, PDFmemberid = self.findPDFfromtarball()
    if PDFname not in self.desiredPDFnames:
      raise ValueError("Wrong PDF!  Tarball has {}, should be {}".format(PDFname, " or ".join(self.desiredPDFnames)))
    if PDFmemberid != self.desiredPDFmemberid:
      raise ValueError("Wrong PDF member id!  Tarball has {}, should be {}".format(PDFmemberid, self.desiredPDFmemberid))
    return super(Run2UltraLegacyBase, self).cardsurl

  @abc.abstractproperty
  def desiredPDFnames(self): pass
  @property
  def desiredPDFmemberid(self): return 0

  @property
  def defaultnthreads(self): return 8

class Run2UltraLegacyStandardPDF(Run2UltraLegacyBase):
  @abc.abstractproperty
  def desiredPDForder(self): pass

  @property
  def desiredPDFnames(self):
    return {
      "NNLO": {"NNPDF31_nnlo_as_0118_mc_hessian_pdfas"} | ({"NNPDF31_nnlo_hessian_pdfas"} if not self.needPDFtobewellbehavedathighmass else set()),
      "NLO": {"NNPDF31_nlo_hessian_pdfas"},
      "LO": {"NNPDF31_lo_as_0130"},
    }[self.desiredPDForder]

  @property
  def needPDFtobewellbehavedathighmass(self): return True
