#!/usr/bin/env python

import contextlib, collections, csv, filecmp, glob, os, random, re, shutil, stat, subprocess, sys, urllib

from utilities import cache, cd, cdtemp, rm_f, jobended, JsonDict, KeepWhileOpenFile, LSB_JOBID, mkdir_p, \
                      mkdtemp, NamedTemporaryFile, restful, TFile, wget

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
    return hash((self.productionmode, self.decaymode, self.mass))
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
    v = 1

    v+=1 #JHUGen version
    if self.productionmode == "ggH" and self.decaymode == "2l2nu" and self.mass == 400: v+=1
    if self.productionmode == "ggH" and self.decaymode == "4l" and self.mass in (300, 350, 400, 450, 500, 550, 600, 700, 750, 800, 900, 1000, 1500, 2000, 2500, 3000): v+=1  #core dumps in v2
    if self.productionmode == "ggH" and self.decaymode == "2l2nu" and self.mass in (300, 400, 1000, 1500): v+=1   #core dumps in v1
    if self.productionmode == "ggH" and self.decaymode == "2l2q" and self.mass == 750: v+=1   #core dumps in v1
    if self.productionmode == "ZH" and self.decaymode == "4l" and self.mass == 145: v+=1   #core dumps in v2
    if self.decaymode == "4l": v+=1  #v1 messed up the JHUGen decay card
    if self.productionmode == "ggH" and self.decaymode == "2l2nu" and self.mass == 2500: v+=1  #v1 is corrupted
    if self.productionmode == "ggH" and self.decaymode == "2l2q" and self.mass == 800: v+=1  #same

    return v

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
    if self.productionmode == "ggH": return "1nd"
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

  @cache
  def checkcardsurl(self):
    try:
      self.cardsurl
    except Exception as e:
      if str(self) in str(e):
        return str(e).replace(str(self), "").strip()
      else:
        raise

  def makegridpack(self, requestqueue=None):
    workdir = os.path.dirname(self.tmptarball)
    if os.path.exists(self.cvmfstarball):
      if os.path.exists(self.foreostarball):
        if filecmp.cmp(self.cvmfstarball, self.foreostarball):
          os.remove(self.foreostarball)
          self.needsupdate = True
        else:
          return "gridpack exists on cvmfs, but it's wrong!"

      if self.matchefficiency is None or self.matchefficiencyerror is None:
        if self.checkcardsurl(): return self.checkcardsurl() #if the cards are wrong, catch it now!
        #figure out the filter efficiency
        if "filter" not in self.JHUGencard.lower():
          self.matchefficiency, self.matchefficiencyerror = 1, 0
        else:
          mkdir_p(workdir)
          jobsrunning = False
          eventsprocessed = eventsaccepted = 0
          with cd(workdir):
            for i in range(100):
              mkdir_p(str(i))
              with cd(str(i)), KeepWhileOpenFile("cmsgrid_final.lhe.tmp", message=LSB_JOBID(), deleteifjobdied=True) as kwof:
                if not kwof:
                  jobsrunning = True
                  continue
                if not os.path.exists("cmsgrid_final.lhe"):
                  if not LSB_JOBID(): return "need to figure out filter efficiency, please run on a queue"
                  with cdtemp():
                    subprocess.check_call(["tar", "xvzf", self.cvmfstarball])
                    with open("powheg.input") as f:
                      powheginput = f.read()
                    powheginput = re.sub("^(rwl_|lhapdf6maxsets)", r"#\1", powheginput, flags=re.MULTILINE)
                    with open("powheg.input", "w") as f:
                      f.write(powheginput)
                    subprocess.check_call(["./runcmsgrid.sh", "1000", str(abs(hash(self))%2147483647 + i), "1"])
                    shutil.move("cmsgrid_final.lhe", os.path.join(workdir, str(i), ""))
                with open("cmsgrid_final.lhe") as f:
                  for line in f:
                    if "events processed:" in line: eventsprocessed += int(line.split()[-1])
                    if "events accepted:" in line: eventsaccepted += int(line.split()[-1])

            if jobsrunning: return "some filter efficiency jobs are still running"
            self.matchefficiency = 1.0*eventsaccepted / eventsprocessed
            self.matchefficiencyerror = (1.0*eventsaccepted * (eventsprocessed-eventsaccepted) / eventsprocessed**3) ** .5
            #shutil.rmtree(workdir)
            return "match efficiency is measured to be {} +/- {}".format(self.matchefficiency, self.matchefficiencyerror)

      if not requestqueue:
        if self.checkcardsurl(): return self.checkcardsurl() #if the cards are wrong, catch it now!
        if self.matchefficiency != 1:
          return "match efficiency is measured to be {} +/- {}".format(self.matchefficiency, self.matchefficiencyerror)
        else:
          return "gridpack exists on cvmfs"

      if self.prepid is None:
        self.getprepid()
        if self.prepid is None:
          #need to make the request
          requestqueue.addrequest(self, useprepid=False)
          return "will send the request to McM, run again to proceed further"
        else:
          return "found prepid: {}".format(self.prepid)

      if not (self.sizeperevent and self.timeperevent):
        if self.needsupdate: return "need update before getting time and size per event, please run ./fixgridpacks.py"
        mkdir_p(workdir)
        with KeepWhileOpenFile(os.path.join(workdir, self.prepid+".tmp"), message=LSB_JOBID(), deleteifjobdied=True) as kwof:
          if not kwof: return "job to get the size and time is already running"
          if not LSB_JOBID(): return "need to get time and size per event, please run on a queue"
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

        shutil.rmtree(workdir)

        if not (self.sizeperevent and self.timeperevent):
          return "failed to get the size and time"
        requestqueue.addrequest(self, useprepid=True)
        return "size and time per event are found to be {} and {}, will send it to McM".format(self.sizeperevent, self.timeperevent)

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
        if jobended(str(jobid)):
          if jobended("-J", "full_"+os.path.basename(self.powhegcard).replace(".input", "")):
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
      result = MCSample(self.productionmode, self.decaymode, 230).datasetname.replace("M230", "M{:d}".format(self.mass))
    else:
      result = self.olddatasetname.replace("JHUgenV698", "JHUGenV709").replace("JHUgenV6", "JHUGenV709")

    pm = self.productionmode.replace("gg", "GluGlu")
    dm = self.decaymode.upper().replace("NU", "Nu")
    if self.decaymode == "2l2q" and self.mass == 125:
      if self.productionmode in ("VBF", "WplusH", "WminusH", "bbH", "tqH"): dm = "2L2X"
      if self.productionmode in ("ZH", "ttH"): dm = "Filter"
    searchfor = [pm, dm, "M{:d}".format(self.mass), "JHUGenV709_"]
    if any(_ not in result for _ in searchfor):
      raise ValueError("Dataset name doesn't make sense:\n{}\n{}\n{}".format(result, searchfor, self))

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
    return r"powheg\ {} JHUGen\ v7.0.9".format(self.powhegprocess)

  @property
  @cache
  def cardsurl(self):
    powhegdir, powhegcard = os.path.split(self.powhegcard)
    powhegscript = os.path.join(powhegdir, "makecards.py")
    commit = "118144fc626bc493af2dac01c57ff51ea56562c7"
    powhegscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, powhegscript.split("genproductions/")[-1])
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.JHUGencard.split("genproductions/")[-1])

    result = (       powhegscript + "\n"
            + "#    " + powhegcard + "\n"
            + "# " + JHUGencard)

    with cdtemp():
      wget(powhegscript)
      wget(os.path.join(os.path.dirname(powhegscript), powhegcard.replace("M{}".format(self.mass), "template").replace("Wplus", "W").replace("Wminus", "W")))
      subprocess.check_call(["python", "makecards.py"])
      with open(powhegcard) as f:
        powheggitcard = f.read()
        powheggitcardlines = [re.sub(" *([#!].*)?$", "", line) for line in powheggitcard.split("\n")]
        powheggitcardlines = [re.sub("(iseed|ncall2|fakevirt) *", r"\1 ", line) for line in powheggitcardlines
                              if line and all(_ not in line for _ in
                              ("pdfreweight", "storeinfo_rwgt", "withnegweights", "rwl_", "lhapdf6maxsets", "xgriditeration")
                              )]
        powheggitcard = "\n".join(line for line in powheggitcardlines)
      with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
        JHUGengitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvzf", self.cvmfstarball])
      if glob.glob("core.*"):
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open("powheg.input") as f:
          powhegcard = f.read()
          powhegcardlines = [re.sub(" *([#!].*)?$", "", line) for line in powhegcard.split("\n")]
          powhegcardlines = [re.sub("(iseed|ncall2|fakevirt) *", r"\1 ", line) for line in powhegcardlines
                             if line and all(_ not in line for _ in
                             ("pdfreweight", "storeinfo_rwgt", "withnegweights", "rwl_", "lhapdf6maxsets", "xgriditeration")
                             )]
          powhegcard = "\n".join(line for line in powhegcardlines)
      except IOError:
        raise ValueError("no powheg.input in the tarball\n{}".format(self))
      try:
        with open("JHUGen.input") as f:
          JHUGencard = f.read()
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}".format(self))

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

  @property
  def nfinalparticles(self):
    if self.productionmode == "ggH": return 1
    if self.productionmode in ("VBF", "ZH", "WplusH", "WminusH", "ttH"): return 3
    assert False, self.productionmode

  @property
  def defaulttimeperevent(self):
    if self.productionmode in ("ggH", "VBF"): return 30
    if self.productionmode in ("WplusH", "WminusH"): return 50
    if self.productionmode == "ZH":
      if self.decaymode == "4l": return 120
      if self.decaymode == "2l2q": return 140
    if self.productionmode == "ttH":
      if self.decaymode == "4l": return 30 #?
      if self.decaymode == "2l2q": return 60
    assert False

  @property
  def tags(self):
    result = ["HZZ"]
    if self.productionmode in ("ggH", "VBF", "ZH", "WplusH", "WminusH", "ttH") and self.decaymode == "4l" and self.mass in (120, 125, 130):
      result.append("Fall17P1S")
    else:
      result.append("Fall17P2A")
    return " ".join(result)

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
      "fragment name": "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_powhegEmissionVeto_{:d}p_LHE_pythia8_cff.py".format(self.nfinalparticles),
      "fragment tag": "118144fc626bc493af2dac01c57ff51ea56562c7",
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

class RequestQueue(object):
  def __init__(self):
    pass
  def __enter__(self):
    self.csvlines = []
    self.requests = []
    self.requeststoapprove = collections.defaultdict(list)
    return self
  def addrequest(self, request, **kwargs):
    if request.prepid is not None and not kwargs.get("useprepid"):
      raise RuntimeError("Request {} is already made!".format(request))
    self.csvlines.append(request.csvline(**kwargs))
    if not os.path.exists(os.path.expanduser("~/private/prod-cookie.txt")):
      raise RuntimeError("Have to run\n  source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh\nprior to doing cmsenv")
    self.requests.append(request)
  def validate(self, request):
    self.requeststoapprove[1].append(request)
  def define(self, request):
    self.requeststoapprove[2].append(request)
  def reset(self, request):
    self.requeststoapprove[0].append(request)
  def __exit__(self, *errorstuff):
    if LSB_JOBID(): return
    keylists = {frozenset(line.keys()) for line in self.csvlines}
    for keys in keylists:
      with contextlib.closing(NamedTemporaryFile(bufsize=0)) as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        for line in self.csvlines:
          if frozenset(line.keys()) == keys:
            writer.writerow(line)
        try:
          command = ["McMScripts/manageRequests.py", "--pwg", "HIG", "-c", "RunIIFall17wmLHEGS", f.name]
          if "prepid" in keys: command.append("-m")
          output = subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
          output = e.output
          raise
        except:
          output = ""
        finally:
          print output,
        if "failed to be created" in output or "failed to be modified" in output:
          raise RuntimeError("Failed to create/modify request")
    for request in self.requests:
      request.needsupdate = False
      request.resettimeperevent = False
    del self.csvlines[:], self.requests[:]

    for level, requests in self.requeststoapprove.iteritems():
      for request in requests:
        restful().approve("requests", request.prepid, level)
    self.requeststoapprove.clear()

def makegridpacks():
  with RequestQueue() as queue:
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2nu", "2l2q":
        for mass in getmasses(productionmode, decaymode):
          sample = MCSample(productionmode, decaymode, mass)
          print sample, sample.makegridpack(queue)
          sys.stdout.flush()

if __name__ == "__main__":
  makegridpacks()
