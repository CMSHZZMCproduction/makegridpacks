import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from powhegmcsample import POWHEGMCSample

class POWHEGJHUGenMCSample(POWHEGMCSample):
  @abc.abstractproperty
  def decaycard(self): pass
  @property
  def hasfilter(self): return "filter" in self.decaycard.lower()
  @property
  def makegridpackcommand(self):
    return super(POWHEGJHUGenMCSample, self).makegridpackcommand + ["-g", self.decaycard]
  @property
  def foldernameforrunpwg(self):
    return super(POWHEGJHUGenMCSample, self).foldernameforrunpwg+"_"+self.decaymode

  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])
    result = super(POWHEGJHUGenMCSample, self).cardsurl + "\n# " + JHUGencard

    with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
      JHUGengitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      if glob.glob("core.*") and self.cvmfstarball != "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/HJJ_M125_13TeV/HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_13TeV_M125.tgz":
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open("JHUGen.input") as f:
          JHUGencard = f.read()
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}".format(self))

    if JHUGencard != JHUGengitcard:
      raise ValueError("JHUGencard != JHUGengitcard\n{}\n{}\n{}".format(self, JHUGencard, JHUGengitcard))

    return result

  @property
  def generators(self):
    return super(POWHEGJHUGenMCSample, self).generators + ["JHUGen v7.0.11"]

class JHUGenFilter(object):
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
        if "events processed:" in line: eventsprocessed += int(line.split()[-1])
        if "events accepted:" in line: eventsaccepted += int(line.split()[-1])

