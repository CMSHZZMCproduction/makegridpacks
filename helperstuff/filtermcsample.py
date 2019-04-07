import abc, os, re, shutil, stat, subprocess, textwrap

from utilities import cdtemp, wget

from mcsamplebase import MCSampleBase

class FilterImplementation(MCSampleBase):
  implementsfilter = True
  @abc.abstractmethod
  def filterjobscript(self, jobindex): "returns the contents of a script (starting with #!) that will be run on condor"
  @abc.abstractmethod
  def getfilterresults(self, jobindex): pass
  @abc.abstractproperty
  def filterresultsfile(self): pass

class GenericFilter(FilterImplementation):
  def __init__(self, *args, **kwargs):
    self.__sizesperevent = {}
    self.__timesperevent = {}
    self.__nevents = {}
    return super(GenericFilter, self).__init__(*args, **kwargs)

  @property
  def filterresultsfile(self):
    return self.prepid+"_rt.xml"

  def filterjobscript(self, jobindex):
    olddir = os.getcwd()
    with cdtemp():
      wget(os.path.join("https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_test/", self.prepid, str(self.neventsfortest) if self.neventsfortest else "").rstrip("/"), output=self.prepid)
      with open(self.prepid) as f:
        testjob = f.read()
        try:
          testjob = eval(testjob)  #sometimes it's a string within a string
        except SyntaxError:
          pass                     #sometimes it's not
      lines = testjob.split("\n")
      cmsdriverindex = {i for i, line in enumerate(lines) if "cmsDriver.py" in line}
      assert len(cmsdriverindex) == 1, cmsdriverindex
      cmsdriverindex = cmsdriverindex.pop()
      lines.insert(cmsdriverindex+1, 'sed -i "/Services/aprocess.RandomNumberGeneratorService.externalLHEProducer.initialSeed = {}" *_cfg.py'.format(abs(hash(self))%900000000 + jobindex))  #The CLHEP::HepJamesRandom engine seed should be in the range 0 to 900000000.
      return "\n".join(lines)

  def getfilterresults(self, jobindex):
    with open(self.filterresultsfile) as f:
      for line in f:
        line = line.strip()
        match = re.match("<TotalEvents>([0-9]*)</TotalEvents>", line)
        if match:
          nevents = int(match.group(1))
          yield nevents
        match = re.match('<Metric Name="Timing-tstoragefile-write-totalMegabytes" Value="([0-9.]*)"/>', line)
        if match: totalsize = float(match.group(1))
        if self.year >= 2017:
          match = re.match('<Metric Name="EventThroughput" Value="([0-9.eE+-]*)"/>', line)
          if match: self.__timesperevent[jobindex] = 1/float(match.group(1))
        else:
          match = re.match('<Metric Name="AvgEventTime" Value="([0-9.eE+-]*)"/>', line)
          if match: self.__timesperevent[jobindex] = float(match.group(1))
    try:
      self.__sizesperevent[jobindex] = totalsize * 1024 / nevents
      self.__nevents[jobindex] = nevents
    except NameError:
      try:
        line
      except NameError: #file is empty
        os.remove(self.filterresultsfile)
        yield None
        yield None
      else:
        raise IOError("File is corrupted: "+os.path.abspath(self.filterresultsfile))

  def findfilterefficiency(self):
    result = super(GenericFilter, self).findfilterefficiency()
    if self.filterefficiency is not None:
      if self.sizeperevent is None and len(self.__sizesperevent) == 100:
        self.sizeperevent = sum(self.__sizesperevent[i] * self.__nevents[i] for i in range(100)) / sum(self.__nevents[i] for i in range(100))
      if self.timeperevent is None and len(self.__timesperevent) == 100:
        self.timeperevent = sum(self.__timesperevent[i] * self.__nevents[i] for i in range(100)) / sum(self.__nevents[i] for i in range(100))
    return result

  @property
  def nthreadsforfilter(self): return self.nthreads

class JHUGenFilter(GenericFilter):
  def filterjobscript(self, jobindex):
    if self.hasnonJHUGenfilter: return super(JHUGenFilter, self).filterjobscript(jobindex)
    oldpath = os.path.join(os.getcwd(), "")
    return textwrap.dedent("""\
    #!/usr/bin/env python

    import os, subprocess, re

    subprocess.check_call(["tar", "xvaf", "{self.cvmfstarball}"])
    if os.path.exists("powheg.input"):
      with open("powheg.input") as f:
        powheginput = f.read()
      powheginput = re.sub("^(rwl_|lhapdf6maxsets)", r"#\1", powheginput, flags=re.MULTILINE)
      with open("powheg.input", "w") as f:
        f.write(powheginput)
    subprocess.check_call(["./runcmsgrid.sh", "1000", "{seed}", "1"])
    """).format(self=self, seed=str(abs(hash(self))%2147483647 + jobindex))

  @property
  def filterresultsfile(self):
    if self.hasnonJHUGenfilter: return super(JHUGenFilter, self).filterresultsfile
    return "cmsgrid_final.lhe"
  def getfilterresults(self, jobindex):
    if self.hasnonJHUGenfilter: return super(JHUGenFilter, self).getfilterresults(jobindex)
    with open(self.filterresultsfile) as f:
      for line in f:
        if "events processed:" in line: eventsprocessed = int(line.split()[-1])
        if "events accepted:" in line: eventsaccepted = int(line.split()[-1])

    try:
      return eventsprocessed, eventsaccepted
    except NameError:
      try:
        line
      except NameError: #file is empty
        os.remove(self.filterresultsfile)
        return None, None
      else:
        raise IOError("File is corrupted: "+os.path.abspath(self.filterresultsfile))

  @abc.abstractproperty
  def hasnonJHUGenfilter(self): pass

  @property
  def nthreadsforfilter(self):
    if self.hasnonJHUGenfilter: return super(JHUGenFilter, self).nthreadsforfilter
    return 1
