import abc, os, re, shutil, subprocess

from utilities import cdtemp, wget

from mcsamplebase import MCSampleBase

class FilterImplementation(MCSampleBase):
  implementsfilter = True
  @abc.abstractmethod
  def dofilterjob(self, jobindex): pass
  @abc.abstractmethod
  def getfilterresults(self, jobindex): pass
  @abc.abstractproperty
  def filterresultsfile(self): pass

class GenericFilter(FilterImplementation):
  def __init__(self, *args, **kwargs):
    self.__sizeperevent = self.__timeperevent = None
    return super(GenericFilter, self).__init__(*args, **kwargs)

  @property
  def filterresultsfile(self):
    return self.prepid+"_rt.xml"

  def dofilterjob(self, jobindex):
    olddir = os.getcwd()
    with cdtemp():
      wget(os.path.join("https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_test/", self.prepid, str(self.neventsfortest) if self.neventsfortest else "").rstrip("/"), output=self.prepid)
      with open(self.prepid) as f:
        testjob = f.read()
      with open(self.prepid, "w") as newf:
        newf.write(eval(testjob))
      os.chmod(self.prepid, os.stat(self.prepid).st_mode | stat.S_IEXEC)
      subprocess.check_call(["./"+self.prepid], stderr=subprocess.STDOUT)
      shutil.move(self.prepid+"_rt.xml", olddir)

  def getfilterresults(self, jobindex):
    with open(self.prepid+"_rt.xml") as f:
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
          if match: self.__timeperevent = 1/float(match.group(1))
        else:
          match = re.match('<Metric Name="AvgEventTime" Value="([0-9.eE+-]*)"/>', line)
          if match: self.__timeperevent = float(match.group(1))
        if nevents is not None is not totalsize:
          self.__sizeperevent = totalsize * 1024 / nevents

  def findmatchefficiency(self):
    result = super(GenericFilter, self).findmatchefficiency()
    if self.matchefficiency is not None is not self.matchefficiencyerror:
      if self.sizeperevent is None is not self.__sizeperevent: self.sizeperevent = self.__sizeperevent
      if self.timeperevent is None is not self.__timeperevent: self.timeperevent = self.__timeperevent
    return result

class JHUGenFilter(FilterImplementation):
  def dofilterjob(self, jobindex):
    oldpath = os.path.join(os.getcwd(), "")
    with cdtemp():
      subprocess.check_call(["tar", "xvaf", self.cvmfstarball])
      if os.path.exists("powheg.input"):
        with open("powheg.input") as f:
          powheginput = f.read()
        powheginput = re.sub("^(rwl_|lhapdf6maxsets)", r"#\1", powheginput, flags=re.MULTILINE)
        with open("powheg.input", "w") as f:
          f.write(powheginput)
      subprocess.check_call(["./runcmsgrid.sh", "1000", str(abs(hash(self))%2147483647 + jobindex), "1"])
      shutil.move("cmsgrid_final.lhe", oldpath)

  @property
  def filterresultsfile(self):
    return "cmsgrid_final.lhe"
  def getfilterresults(self, jobindex):
    with open("cmsgrid_final.lhe") as f:
      for line in f:
        if "events processed:" in line: eventsprocessed = int(line.split()[-1])
        if "events accepted:" in line: eventsaccepted = int(line.split()[-1])
    return eventsprocessed, eventsaccepted
