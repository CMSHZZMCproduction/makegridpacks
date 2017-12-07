import abc, filecmp, glob, os, re, shutil, stat, subprocess

class MCSampleBase(JsonDict):
  @abc.abstractmethod
  def __init__(self): pass
  @abc.abstractproperty
  def identifiers(self):
    """example: productionmode, decaymode, mass"""
  @abc.abstractproperty
  def tarballversion(self): pass
  @abc.abstractproperty
  def cvmfstarball(self): pass
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
  @abc.abstractproperty
  def nevents(self): pass
  @abc.abstractproperty
  def generators(self): pass
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

  def __eq__(self, other):
    return self.keys == other.keys
  def __ne__(self, other):
    return not (self == other)
  def __hash__(self):
    return hash(self.keys)
  def __str__(self):
    return " ".join(str(_) for _ in self.identifiers)
  def __repr__(self):
    return type(self).__name__+"(" +  ", ".join(repr(_) for _ in self.identifiers) + ")"

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
    return os.path.dirname(self.tmptarball)

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
            if jobended("-J", self.makinggridpacksubmitsjob):
              for _ in os.listdir("."):            #--> delete everything in the folder, except the tarball if that exists
                if os.path.basename(_) != os.path.basename(self.tmptarball) and os.path.basename(_) != os.path.basename(self.tmptarball)+".tmp":
                  try:
                    os.remove(_)
                  except OSError:
                    shutil.rmtree(_)
              os.remove(os.path.basename(self.tmptarball)+".tmp") #remove that last
              return "gridpack job died, cleaned it up.  run makegridpacks.py again."
            else:
              return "job to make the tarball is already running (but the original one died)"
        else:
          return "job to make the tarball is already running"

      if not os.path.exists(self.tmptarball):
        for _ in os.listdir("."):
          if not _.endswith(".tmp"):
            try:
              os.remove(_)
            except OSError:
              shutil.rmtree(_)
        if not LSB_JOBID(): requestqueue.submitLSF(self); return "need to create the gridpack, submitting to LSF"
        for filename in glob.iglob(os.path.join(genproductions, "bin", "Powheg", "*")):
          if (filename.endswith(".py") or filename.endswith(".sh") or filename == "patches") and not os.path.exists(os.path.basename(filename)):
            os.symlink(filename, os.path.basename(filename))
        output = subprocess.check_output(self.makegridpackcommand)
        print output
        if self.makinggridpacksubmitsjob:
          waitids = []
          for line in output.split("\n"):
            if "is submitted to" in line:
              waitids.append(int(line.split("<")[1].split(">")[0]))
          if waitids:
            subprocess.check_call(["bsub", "-q", "cmsinter", "-I", "-J", "wait for "+str(self), "-w", " && ".join("ended({})".format(_) for _ in waitids), "echo", "done"])
          else:
            for _ in os.listdir("."):
              if not _.endswith(".tmp"):
                try:
                  os.remove(_)
                except OSError:
                  shutil.rmtree(_)
            return "gridpack job submission failed"

    mkdir_p(os.path.dirname(self.foreostarball))
    shutil.move(self.tmptarball, self.foreostarball)
    shutil.rmtree(os.path.dirname(self.tmptarball))
    return "tarball is created and moved to this folder, to be copied to eos"

  def findmatchefficiency(self):
    if self.checkcardsurl(): return self.checkcardsurl() #if the cards are wrong, catch it now!
    #figure out the filter efficiency
    if not self.hasfilter:
      self.matchefficiency, self.matchefficiencyerror = 1, 0
      return "filter efficiency is set to 1 +/- 0"
    else:
      mkdir_p(self.workdir)
      jobsrunning = False
      eventsprocessed = eventsaccepted = 0
      with cd(self.workdir):
        for i in range(100):
          mkdir_p(str(i))
          with cd(str(i)), KeepWhileOpenFile("cmsgrid_final.lhe.tmp", message=LSB_JOBID(), deleteifjobdied=True) as kwof:
            if not kwof:
              jobsrunning = True
              continue
            if not os.path.exists("cmsgrid_final.lhe"):
              if not LSB_JOBID(): requestqueue.submitLSF(self); return "need to figure out filter efficiency, submitting to LSF"
              with cdtemp():
                subprocess.check_call(["tar", "xvzf", self.cvmfstarball])
                with open("powheg.input") as f:
                  powheginput = f.read()
                powheginput = re.sub("^(rwl_|lhapdf6maxsets)", r"#\1", powheginput, flags=re.MULTILINE)
                with open("powheg.input", "w") as f:
                  f.write(powheginput)
                subprocess.check_call(["./runcmsgrid.sh", "1000", str(abs(hash(self))%2147483647 + i), "1"])
                shutil.move("cmsgrid_final.lhe", os.path.join(self.workdir, str(i), ""))
            with open("cmsgrid_final.lhe") as f:
              for line in f:
                if "events processed:" in line: eventsprocessed += int(line.split()[-1])
                if "events accepted:" in line: eventsaccepted += int(line.split()[-1])

        if jobsrunning: return "some filter efficiency jobs are still running"
        self.matchefficiency = 1.0*eventsaccepted / eventsprocessed
        self.matchefficiencyerror = (1.0*eventsaccepted * (eventsprocessed-eventsaccepted) / eventsprocessed**3) ** .5
        #shutil.rmtree(self.workdir)
        return "match efficiency is measured to be {} +/- {}".format(self.matchefficiency, self.matchefficiencyerror)

  def getsizeandtime(self):
    mkdir_p(self.workdir)
    with KeepWhileOpenFile(os.path.join(self.workdir, self.prepid+".tmp"), message=LSB_JOBID(), deleteifjobdied=True) as kwof:
      if not kwof: return "job to get the size and time is already running"
      if not LSB_JOBID(): requestqueue.submitLSF(self); return "need to get time and size per event, submitting to LSF"
      with cdtemp():
        wget("https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_test/"+self.prepid)
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
            match = re.match('<Metric Name="AvgEventTime" Value="([0-9.]*)"/>', line)
            if match: self.timeperevent = float(match.group(1))
          if nevents is not None is not totalsize:
            self.sizeperevent = totalsize * 1024 / nevents

    shutil.rmtree(self.workdir)

    if not (self.sizeperevent and self.timeperevent):
      return "failed to get the size and time"
    requestqueue.addrequest(self, useprepid=True)
    return "size and time per event are found to be {} and {}, will send it to McM".format(self.sizeperevent, self.timeperevent)

  def makegridpack(self, requestqueue=None):
    if not os.path.exists(self.cvmfstarball):
      if not os.path.exists(self.eostarball):
        if not os.path.exists(self.foreostarball):
          return self.createtarball()
        return "gridpack exists in this folder, to be copied to eos"
      return "gridpack exists on eos, not yet copied to cvmfs"

    if os.path.exists(self.foreostarball):
      if filecmp.cmp(self.cvmfstarball, self.foreostarball, shallow=False):
        os.remove(self.foreostarball)
        self.needsupdate = True
      else:
        return "gridpack exists on cvmfs, but it's wrong!"

    if self.matchefficiency is None or self.matchefficiencyerror is None:
      return self.findmatchefficiency()

    if not requestqueue:
      if self.checkcardsurl(): return self.checkcardsurl() #if the cards are wrong, catch it now!
      return "gridpack exists on cvmfs, and the match efficiency is {} +/- {}".format(self.matchefficiency, self.matchefficiencyerror)

    if self.prepid is None:
      self.getprepid()
      if self.prepid is None:
        #need to make the request
        requestqueue.addrequest(self, useprepid=False)
        return "will send the request to McM, run again to proceed further"
      else:
        return "found prepid: {}".format(self.prepid)

    if not (self.sizeperevent and self.timeperevent):
      if self.needsupdate:
        requestqueue.addrequest(self, useprepid=True)
        return "need update before getting time and size per event, sending the request to McM"
      return self.getsizeandtime()

    if LSB_JOBID():
      return "please run locally to check and/or advance the status".format(self.prepid)

    if (self.approval, self.status) == ("none", "new"):
      if self.needsupdate:
        requestqueue.addrequest(self, useprepid=True)
        return "needs update on McM, sending it there"
      requestqueue.validate(self)
      return "starting the validation"
    if (self.approval, self.status) == ("validation", "new"):
      return "validation is running"
    if (self.approval, self.status) == ("validation", "validation"):
      if self.needsupdate:
        requestqueue.reset(self)
        return "needs update on McM, resetting the request"
      requestqueue.define(self)
      return "defining the request"
    if (self.approval, self.status) == ("define", "defined"):
      if self.needsupdate:
        requestqueue.reset(self)
        return "needs update on McM, resetting the request"
      return "request is defined"
    return "Unknown approval "+self.approval+" and status "+self.status

  #these things should all be calculated once.
  #they are stored in McMsampleproperties.json in this folder.
  #see JsonDict in utilities for how that works
  @property
  def keys(self):
    return tuple(str(_) for _ in self.identifiers)
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
    self.needsupdate = True
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
    self.needsupdate = True
  @sizeperevent.deleter
  def sizeperevent(self):
    with cd(here), self.writingdict():
      del self.value["sizeperevent"]
  @property
  def matchefficiency(self):
    with cd(here):
      return self.value.get("matchefficiency")
  @matchefficiency.setter
  def matchefficiency(self, value):
    with cd(here), self.writingdict():
      self.value["matchefficiency"] = value
    self.needsupdate = True
  @matchefficiency.deleter
  def matchefficiency(self):
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
    with cd(here):
      return self.value.get("needsupdate", True)
  @needsupdate.setter
  def needsupdate(self, value):
    with cd(here), self.writingdict():
      self.value["needsupdate"] = value
  @needsupdate.deleter
  def needsupdate(self):
    with cd(here), self.writingdict():
      del self.value["needsupdate"]

  @property
  def filterefficiency(self): return 1
  @property
  def filterefficiencyerror(self): return 0

  def csvline(self, useprepid):
    result = {
      "dataset name": self.datasetname,
      "xsec [pb]": 1,
      "total events": self.nevents,
      "generator": self.generators,
      "match efficiency": self.matchefficiency*self.filterefficiency,
      "match efficiency error": ((self.matchefficiencyerror*self.filterefficiency)**2 + (self.matchefficiency*self.filterefficiencyerror)**2)**.5,
      "pwg": "HIG",
      "campaign": "RunIIFall17wmLHEGS",
      "gridpack location": self.cvmfstarball,
      "cards url": self.cardsurl,
      "fragment name": self.fragmentname,
      "fragment tag": self.genproductionscommit,
      "mcm tag": self.tags,
      "mcdbid": 0,
      "time per event [s]": self.timeperevent if self.timeperevent is not None else self.defaulttimeperevent,
      "size per event [kb]": self.sizeperevent if self.sizeperevent is not None else 600,
      "Sequences nThreads": 1,
    }
    if useprepid: result["prepid"] = self.prepid

    return result

  def getprepid(self):
    if LSB_JOBID(): return
    output = subprocess.check_output(["McMScripts/getRequests.py", "dataset_name={}&prepid=HIG-RunIIFall17wmLHEGS-*".format(self.datasetname), "-bw"])
    if "Traceback (most recent call last):" in output:
      raise RuntimeError(output)
    lines = {_ for _ in output.split("\n") if "HIG-" in _ and "&prepid=HIG-" not in _}
    try:
      line = lines.pop()
    except KeyError:
      return None
    if lines:
      raise RuntimeError("Don't know what to do with this output:\n\n"+output)
    prepids = set(line.split(","))
    if len(prepids) != 1:
      raise RuntimeError("Multiple prepids for {} (dataset_name={}&prepid=HIG-RunIIFall17wmLHEGS-*)".format(self, self.datasetname))
    assert len(prepids) == 1, prepids
    self.prepid = prepids.pop()

  @property
  @cache
  def fullinfo(self):
    if not self.prepid: raise ValueError("Can only call fullinfo once the prepid has been set")
    result = restful().getA("requests", query="prepid="+self.prepid)
    if len(result) == 0:
      raise ValueError("mcm query for prepid="+self.prepid+" returned nothing!")
    if len(result) > 1:
      raise ValueError("mcm query for prepid="+self.prepid+" returned multiple results!")
    return result[0]

  def gettimepereventfromMcM(self):
    if self.timeperevent is None or self.resettimeperevent: return
    needsupdate = self.needsupdate
    timeperevent = self.fullinfo["time_event"][0]
    if timeperevent != self.defaulttimeperevent:
      self.timeperevent = timeperevent
      self.needsupdate = needsupdate #don't need to reupdate on McM, unless that was already necessary

  @property
  def approval(self):
    return self.fullinfo["approval"]
  @property
  def status(self):
    return self.fullinfo["status"]
