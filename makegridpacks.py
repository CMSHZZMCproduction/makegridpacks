#!/usr/bin/env python

import contextlib, csv, filecmp, glob, os, random, re, shutil, subprocess, sys, urllib

from utilities import cache, cd, KeepWhileOpenFile, LSB_JOBID, mkdir_p, mkdtemp, TFile

#do not change these once you've started making tarballs!
#they are included in the tarball name and the script
#will think the tarballs don't exist even if they do
cmsswversion = "CMSSW_9_3_0"
scramarch = "slc6_amd64_gcc630"

genproductions = os.path.join(os.environ["CMSSW_BASE"], "src", "genproductions")
if not os.path.exists(genproductions) or os.environ["CMSSW_VERSION"] != cmsswversion or os.environ["SCRAM_ARCH"] != scramarch:
  raise RuntimeError("Need to cmsenv in a " + cmsswversion + " " + scramarch + " release that contains genproductions")

here = os.path.dirname(os.path.abspath(__file__))

class MCSample(JsonDict):
  def __init__(self, productionmode, decaymode, mass):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))

  def __eq__(self, other):
    return self.productionmode == other.productionmode and self.decaymode == other.decaymode and self.mass == other.mass
  def __ne__(self, other):
    return not (self == other)
  def __hash__(self):
    return hash((self.productionmode, self.mass))
  def __str__(self):
    return "{} {} {}".format(self.productionmode, self.decaymode, self.mass)
  def __repr__(self):
    return "{}({!r}, {!r}, {!r})".format(type(self).__name__, self.productionmode, self.decaymode, self.mass)

  @property
  def powhegprocess(self):
    if self.productionmode == "ggH": return "gg_H_quark-mass-effects"
    if self.productionmode == "VBF": return "VBF_H"
    if self.productionmode == "ZH": return "HZJ"
    if self.productionmode in ("WplusH", "WminusH"): return "HWJ"
    if self.productionmode == "ttH": return "ttH"
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def powhegcard(self):
    folder = os.path.join(genproductions, "bin", "Powheg", "production", "2017", "13TeV", self.powhegprocess+"_NNPDF31_13TeV")
    makecards(folder)

    cardbase = self.powhegprocess
    if self.productionmode == "ZH": cardbase = "HZJ_HanythingJ"
    if self.productionmode == "WplusH": cardbase = "HWplusJ_HanythingJ"
    if self.productionmode == "WminusH": cardbase = "HWminusJ_HanythingJ"
    if self.productionmode == "ttH": cardbase = "ttH_inclusive"
    card = os.path.join(folder, cardbase+"_NNPDF31_13TeV_M{:d}.input".format(self.mass))

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def filter4L(self):
    if self.decaymode != "4l": return False
    if self.productionmode in ("ggH", "VBF", "WplusH", "WminusH"): return False
    if self.productionmode in ("ZH", "ttH"): return True
    raise ValueError("Unknown productionmode "+self.productionmode)

  @property
  def reweightdecay(self):
    return self.mass >= 200

  @property
  def JHUGencard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

    if self.decaymode == "4l":
      filename = "ZZ2l2any_withtaus_filter4l" if self.filter4L else "ZZ4l_withtaus"
      if self.reweightdecay: filename += "_reweightdecay_CPS"
      filename += ".input"
    elif self.decaymode == "2l2nu":
      if self.reweightdecay:
        filename = "ZZ2l2nu_notaus_reweightdecay_CPS.input"
    elif self.decaymode == "2l2q":
      if self.mass == 125:
        if self.productionmode == "ggH":
          filename = "ZZ2l2q_withtaus.input"
        elif self.productionmode in ("VBF", "WplusH", "WminusH", "bbH", "tqH"):
          filename = "ZZ2l2any_withtaus.input"
        elif self.productionmode in ("ZH", "ttH"):
          filename = "ZZany_filter2lOSSF.input"
      elif self.reweightdecay:
        filename = "ZZ2l2q_notaus_reweightdecay_CPS.input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def tarballversion(self):
    return 1

  @property
  def cvmfstarball(self):
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2", self.powhegprocess+"_NNPDF31_13TeV")
    tarballname = os.path.basename(self.powhegcard).replace(".input", ".tgz")
    if self.decaymode != "4l":
      decaymode = self.decaymode
      if "ZZ2l2any_withtaus.input" in self.JHUGencard: decaymode == "2l2X"
      elif "ZZany_filter2lOSSF.input" in self.JHUGencard: decaymode = "_filter2l"
      tarballname = tarballname.replace("NNPDF31", "ZZ"+self.decaymode+"_NNPDF31")
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(self.tarballversion), tarballname)

  @property
  def eostarball(self):
    return self.cvmfstarball.replace("/cvmfs/cms.cern.ch/phys_generator/", "/eos/cms/store/group/phys_generator/cvmfs/")

  @property
  def foreostarball(self):
    """to put in a directory structure here, which will later be copied to eos"""
    return self.cvmfstarball.replace("/cvmfs/cms.cern.ch/phys_generator/", here+"/")

  @property
  def workdir(self):
    return os.path.join()

  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode),
             self.powhegprocess+"_"+scramarch+"_"+cmsswversion+"_"+os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode+".tgz"))

  @property
  def queue(self):
    if self.productionmode == "ggH": return "8nh"
    if self.productionmode in ("ZH", "ttH"): return "1nw"
    return "1nd"

  @property
  def makegridpackcommand(self):
    args = {
      "-p": "f",
      "-i": self.powhegcard,
      "-g": self.JHUGencard,
      "-m": self.powhegprocess,
      "-f": os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode),
      "-q": self.queue,
      "-n": "10",
    }

    return ["./run_pwg.py"] + sum(([k, v] for k, v in args.iteritems()), [])

  def makegridpack(self, requestqueue=None):
    workdir = os.path.dirname(self.tmptarball)
    if os.path.exists(self.cvmfstarball):
      if os.path.exists(self.foreostarball):
        if filecmp.cmp(self.cvmfstarball, self.foreostarball):
          os.remove(self.foreostarball)
        else:
          return "gridpack exists on cvmfs, but it's wrong!"

      self.cardsurl #if the cards are wrong, catch it now!
      if self.matchefficiency is None or self.matchefficiencyerror is None:
        #figure out the filter efficiency
        if "filter" not in self.JHUGencard.lower():
          self.matchefficiency, self.matchefficiencyerror = 1, 0
        else:
          mkdir_p(workdir)
          jobsrunning = False
          eventsprocessed = eventsaccepted = 0
          with cd(workdir):
            for i in range(20):
              mkdir_p(str(i))
              with cd(str(i)), KeepWhileOpenFile("cmsgrid_final.lhe.tmp", message=LSB_JOBID()) as kwof:
                if not kwof:
                  jobsrunning = True
                  continue
                if not os.path.exists("cmsgrid_final.lhe"):
                  if not LSB_JOBID(): return "need to figure out filter efficiency, please run on a queue"
                  with cd(mktemp()):
                    subprocess.check_call(["tar", "xvzf", self.cvmfstarball])
                    with open("powheg.input") as f:
                      powheginput = f.read()
                    powheginput = re.sub("pdfreweight *1", "pdfreweight 0", powheginput)
                    powheginput = re.sub("storeinfo_rwgt *1", "storeinfo_rwgt 0", powheginput)
                    with open("powheg.input", "w") as f:
                      f.write(powheginput)
                    subprocess.check_call(["./runcmsgrid.sh", "10000", str(hash(self)+i), "1"])
                    shutil.move("cmsgrid_final.lhe", os.path.join(workdir, str(i)))
                with open("cmsgrid_final.lhe") as f:
                  for line in f:
                    if "events processed:" in line: eventsprocessed += int(line.split()[-1])
                    if "events accepted:" in line: eventsaccepted += int(line.split()[-1])

            if jobsrunning: return "some filter efficiency jobs are still running"
            self.matchefficiency = 1.0*eventsaccepted / eventsprocessed
            self.matchefficiencyerror = (eventsaccepted * (eventsprocessed-eventsaccepted) / eventsprocessed-eventsaccepted**3) ** .5
            return "match efficiency is measured to be {} +/- {}".format(self.matchefficiency, self.matchefficiencyerror)
          
      if not requestqueue:
        if self.matchefficiency != 1:
          return "match efficiency is measured to be {} +/- {}".format(self.matchefficiency, self.matchefficiencyerror)
        else:
          return "gridpack exists on cvmfs"

      if self.prepid is None:
        self.getprepid()
        if self.prepid is None:
          #need to make the request
          requestqueue.makerequest(self)
          return "will send the request to McM, run again to proceed"

    if os.path.exists(self.eostarball): return "gridpack exists on eos, not yet copied to cvmfs"
    if os.path.exists(self.foreostarball): return "gridpack exists in this folder, to be copied to eos"

    mkdir_p(workdir)
    with cd(workdir), KeepWhileOpenFile(self.tmptarball+".tmp", message=LSB_JOBID()) as kwof:
      if not kwof:
        with open(self.tmptarball+".tmp") as f:
          try:
            jobid = int(f.read().strip())
          except ValueError:
            return "try running again, probably you just got really bad timing"
        try:
          bjobsout = subprocess.check_output(["bjobs", str(jobid)], stderr=subprocess.STDOUT)
          if re.match("Job <[0-9]*> is not found", bjobsout.strip()):
            raise subprocess.CalledProcessError(None, None, None)
          lines = bjobsout.strip().split("\n")
          if len(lines) == 2 and lines[1].split()[2] == "EXIT":
            raise subprocess.CalledProcessError(None, None, None)
        except subprocess.CalledProcessError:    #job died
          try:
            bjobsout = subprocess.check_output(["bjobs", "-J", "full_"+os.path.basename(self.powhegcard).replace(".input", "")], stderr=subprocess.STDOUT)
            if re.match("Job <.*> is not found", bjobsout.strip()):
              raise subprocess.CalledProcessError(None, None, None)
          except subprocess.CalledProcessError:  #that job died or ended too
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
        if not LSB_JOBID(): return "need to create the gridpack, please run on a queue"
        for filename in glob.iglob(os.path.join(genproductions, "bin", "Powheg", "*")):
          if (filename.endswith(".py") or filename.endswith(".sh") or filename == "patches") and not os.path.exists(os.path.basename(filename)):
            os.symlink(filename, os.path.basename(filename))
        output = subprocess.check_output(self.makegridpackcommand)
        print output
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

  @property
  @cache
  def olddatasetname(self):
    p = self.productionmode
    if p == "VBF": p = "VBFH"
    with contextlib.closing(urllib.urlopen("https://raw.githubusercontent.com/CJLST/ZZAnalysis/miniAOD_80X/AnalysisStep/test/prod/samples_2016_MC.csv")) as f:
      reader = csv.DictReader(f)
      for row in reader:
        if row["identifier"] == "{}{}".format(p, self.mass):
          dataset = row["dataset"]
          result = re.sub(r"^/([^/]*)/[^/]*/[^/]*$", r"\1", dataset)
          assert result != dataset and "/" not in result, result
          if self.decaymode == "4l":
            return result
    raise ValueError("Nothing for {}".format(self))

  @property
  def datasetname(self):
    if self.decaymode == "2l2nu":
      result = MCSample(self.productionmode, "4l", self.mass).datasetname.replace("4L", "2L2Nu")
    elif self.decaymode == "2l2q":
      result = MCSample(self.productionmode, "4l", self.mass).datasetname.replace("4L", "2L2Q")
      if self.mass == 125:
        if self.productionmode in ("VBF", "WplusH", "WminusH", "bbH", "tqH"): result = result.replace("2L2Q", "2L2X")
        if self.productionmode == "ZH": result = "ZH_HToZZ_2LFilter_M125_13TeV_powheg2-minlo-HZJ_JHUGenV709_pythia8"
        if self.productionmode == "ttH": result = "ttH_HToZZ_2LOSSFFilter_M125_13TeV_powheg2_JHUGenV709_pythia8"
    elif self.productionmode in ("WplusH", "WminusH", "ZH") and self.mass > 230:
      result = MCSample(self.productionmode, self.decaymode, self.mass).datasetname.replace("M230", "M{:d}".format(self.mass))
    else:
      result = self.olddatasetname.replace("JHUgenV6", "JHUGenV709")

    dm = self.decaymode.upper().replace("NU", "Nu")
    if self.decaymode == "2l2q" and self.mass == "125":
      if self.productionmode in ("VBF", "WplusH", "WminusH", "bbH", "tqH"): dm = "2L2X"
      if self.productionmode in ("ZH", "ttH"): dm = "Filter"
    searchfor = [self.productionmode, dm, "M{:d}".format(self.mass), "JHUGenV709"]
    if any(_ not in result.lower() for _ in searchfor):
      raise ValueError("Dataset name doesn't make sense:\n{}\n{}".format(result, self))

    return result

  @property
  def nevents(self):
    if self.decaymode == "4l":
      if self.productionmode == "ggH":
        if 124 <= self.mass <= 126: return 1000000
        return 500000
      elif self.productionmode in ("VBF", "ZH", "ttH", "bbH"):
        if 124 <= self.mass <= 126 or self.mass >= 1500: return 500000
        return 200000
      elif self.productionmode == "WplusH":
        if 124 <= self.mass <= 126: return 300000
        return 180000
      elif self.productionmode == "WminusH":
        if 124 <= self.mass <= 126: return 200000
        return 120000
      elif self.productionmode == "tqH":
        if self.mass == 125: return 1000000
    elif self.decaymode == "2l2nu":
      if self.productionmode in ("ggH", "VBF"):
        if 200 <= self.mass <= 1000: return 250000
        elif self.mass > 1000: return 500000
    elif self.decaymode == "2l2q":
      if self.productionmode == "ggH":
        if self.mass == 125: return 1000000
        elif 200 <= self.mass <= 1000: return 200000
        elif self.mass > 1000: return 500000
      elif self.productionmode == "VBF":
        if self.mass == 125: return 500000
        elif 200 <= self.mass <= 1000: return 100000
        elif self.mass > 1000: return 500000
      elif self.productionmode in ("ZH", "ttH", "bbH", "tqH"):
        if self.mass == 125: return 500000
      elif self.productionmode == "WplusH":
        if self.mass == 125: return 300000
      elif self.productionmode == "WminusH":
        if self.mass == 125: return 200000

    raise ValueError("No nevents for {}".format(self))

  @property
  def generators(self):
    return r"powheg\ {} JHUGen v7.0.9".format(self.powhegprocess)

  @property
  @cache
  def cardsurl(self):
    powhegdir, powhegcard = os.path.split(self.powhegcard)
    powhegscript = os.path.join(powhegdir, "makecards.py")
    commit = "118144fc626bc493af2dac01c57ff51ea56562c7"
    powhegscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, powhegscript.split("genproductions/")[1])
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.JHUGencard.split("genproductions/")[1])

    result = (       powhegscript + "\n"
            + "#    " + powhegcard + "\n"
            + "# " + JHUGencard)

    with cd(mkdtemp()):
      wget(powhegscript)
      wget(os.path.join(os.path.dirname(powhegscript), powhegcard.replace("M{}".format(self.mass), "template"))
      subprocess.check_call(["./makecards.py"])
      with open(powhegcard) as f:
        powheggitcard = f.read()
      with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
        JHUGengitcard = f.read()

    with cd(mkdtemp()):
      subprocess.check_call(["tar", "xvzf", self.cvmfstarball])
      with open("powheg.input") as f:
        powhegcard = f.read()
      with open("JHUGen.input") as f:
        JHUGencard = f.read()

    if powhegcard != powheggitcard:
      with cd(here):
        with open("powhegcard", "w") as f:
          f.write(powhegcard)
        with open("powheggitcard", "w") as f:
          f.write(powheggitcard)
      raise ValueError("powhegcard != powheggitcard\n{}\nSee ./powhegcard and ./powheggitcard".format(self))
    if JHUGencard != JHUGengitcard:
      raise ValueError("JHUGencard != JHUGengitcard\n{}\n{}\n{}".format(self, JHUGencard, JHUGengitcard))

    return result

  #these things should all be calculated once.
  #they are stored in McMsampleproperties.json in this folder.
  #see JsonDict in utilities for how that works
  @property
  def keys(self):
    return self.productionmode, self.decaymode, str(self.mass)
  dictfile = "McMsampleproperties.json"
  @property
  def default(self): return {}

  @property
  def prepid(self): return self.value.get("prepid")
  @prepid.setter
  def prepid(self, value):
    with self.writingdict():
      self.value["prepid"] = value
  @property
  def timeperevent(self): return self.value.get("timeperevent", 100)
  @timeperevent.setter
  def timeperevent(self, value):
    with self.writingdict():
      self.value["timeperevent"] = value
  @property
  def sizeperevent(self): return self.value.get("sizeperevent", 100)
  @sizeperevent.setter
  def sizeperevent(self, value):
    with self.writingdict():
      self.value["sizeperevent"] = value
  @property
  def matchefficiency(self): return self.value.get("matchefficiency", 1)
  @matchefficiency.setter
  def matchefficiency(self, value):
    with self.writingdict():
      self.value["matchefficiency"] = value
  @property
  def matchefficiencyerror(self): return self.value.get("matchefficiencyerror", 0)
  @matchefficiencyerror.setter
  def matchefficiencyerror(self, value):
    with self.writingdict():
      self.value["matchefficiencyerror"] = value

  @property
  def filterefficiency(self): return 1
  @property
  def filterefficiencyerror(self): return 0

  @property
  def csvline(self, useprepid=False):
    print "Getting csv line for", self
    result = {
      "dataset name": self.datasetname,
      "xsec [pb]": 1,
      "total events": self.nevents,
      "time per event [s]": self.timeperevent,
      "size per event [kb]": self.sizeperevent,
      "generator": self.generators,
      "match efficiency": self.matchefficiency*self.filterefficiency,
      "match efficiency error": ((self.matchefficiencyerror*self.filterefficiency)**2 + (self.matchefficiency*self.filterefficiencyerror)**2)**.5,
      "pwg": "HIG",
      "campaign": "RunIIFall17wmLHEGS",
      "gridpack location": self.cvmfstarball,
      "cards url": self.cardsurl,
      "fragment name": "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_powhegEmissionVeto_{:d}p_LHE_pythia8_cff.py".format(self.nfinalparticles),
      "fragment tag": "118144fc626bc493af2dac01c57ff51ea56562c7",
      "mcm tag": "HZZ",
      "mcdbid": 0,
    }
    if useprepid: result["prepid"] = self.prepid
    return result

@cache
def makecards(folder):
  with cd(folder):
    subprocess.check_call(["./makecards.py"])

def getmasses(productionmode, decaymode):
  if decaymode == "4l":
    if productionmode in ("ggH", "VBF", "WplusH", "WminusH", "ZH"):
      return 115, 120, 124, 125, 126, 130, 135, 140, 145, 150, 155, 160, 165, 170, 175, 180, 190, 200, 210, 230, 250, 270, 300, 350, 400, 450, 500, 550, 600, 700, 750, 800, 900, 1000, 1500, 2000, 2500, 3000
    if productionmode == "ttH":
      return 115, 120, 124, 125, 126, 130, 135, 140, 145
  if decaymode == "2l2nu":
    if productionmode in ("ggH", "VBF"):
      return 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000
    if productionmode in ("WplusH", "WminusH", "ZH", "ttH"):
      return ()
  if decaymode == "2l2q":
    if productionmode in ("ggH", "VBF"):
      return 125, 200, 250, 300, 350, 400, 450, 500, 550, 600, 700, 750, 800, 900, 1000, 1500, 2000, 2500, 3000
    if productionmode in ("WplusH", "WminusH", "ZH", "ttH"):
      return 125,

def makegridpacks():
  for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
    for decaymode in "4l", "2l2nu", "2l2q":
     for mass in getmasses(productionmode, decaymode):
      sample = MCSample(productionmode, decaymode, mass)
      print sample, sample.makegridpack()

if __name__ == "__main__":
  makegridpacks()
