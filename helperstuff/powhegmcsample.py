import abc, collections, contextlib, glob, os, re, shutil, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, jobended, makecards, OrderedCounter, scramarch, wget

from mcsamplebase import MCSampleBase

class POWHEGMCSample(MCSampleBase):
  @abc.abstractproperty
  def powhegprocess(self): pass
  @abc.abstractproperty
  def powhegcard(self): pass
  @abc.abstractproperty
  def powhegcardusesscript(self): pass
  @abc.abstractproperty
  def powhegsubmissionstrategy(self): pass
  @property
  def foldernameforrunpwg(self):
    return os.path.basename(self.powhegcard).replace(".input", "")
  @property
  def creategridpackqueue(self):
    if self.powhegsubmissionstrategy == "multicore" and self.multicore_upto[0] == 9: return None
    return super(POWHEGMCSample, self).creategridpackqueue
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", self.foldernameforrunpwg,
             self.powhegprocess+"_"+scramarch+"_"+cmsswversion+"_"+self.foldernameforrunpwg+".tgz")
  @property
  def pwgrwlfilter(self):
    """
    Returns a function that takes an AlternateWeight object (below) and returns True
    if we should keep the weight, False if not.
    """
    return None
  @property
  def patchkwargs(self):
    if self.pwgrwlfilter: return {"functionname": "prunepwgrwl", "filter": self.pwgrwlfilter}
    return super(POWHEGMCSample, self).patchkwargs
  @property
  def makegridpackcommand(self):
    args = {
      "-i": self.powhegcard,
      "-m": self.powhegprocess,
      "-f": self.foldernameforrunpwg,
    }
    if self.powhegsubmissionstrategy == "onestep":
      args.update({
        "-p": "f",
        "-n": "10",
        "-s": str(hash(self) % 2147483647),
        "-q": self.creategridpackqueue,
      })
    elif self.powhegsubmissionstrategy == "multicore":
      if self.multicore_upto[0] == 0:
        args.update({
          "-p": "0",
        })
      elif self.multicore_upto[0] in (1, 2, 3):
        args.update({
          "-p": str(self.multicore_upto[0]),
          "-x": str(self.multicore_upto[1]),
          "-j": "10",
          "-t": "100",
          "-n": "10",
          "-q": self.creategridpackqueue,
        })
        with cd(os.path.join(self.workdirforgridpack, self.foldernameforrunpwg)):
          for _ in glob.iglob("run_{-p}_{-x}_*.*".format(**args)):
            os.remove(_)
      elif self.multicore_upto[0] == 9:
        args.update({
          "-p": "9",
          "-k": "1",
        })
      else:
        assert False, self.multicore_upto
    else:
      assert False, self.powhegsubmissionstrategy
    return ["./run_pwg.py"] + sum(([k, v] for k, v in args.iteritems()), [])

  @property
  def inthemiddleofmultistepgridpackcreation(self):
    if os.path.exists(self.tmptarball): return False
    if self.powhegsubmissionstrategy == "multicore" and self.multicore_upto[0] != 0: return True
    return super(POWHEGMCSample, self).inthemiddleofmultistepgridpackcreation

  @property
  def multicore_upto(self):
    assert self.powhegsubmissionstrategy == "multicore", self.powhegsubmissionstrategy
    if not os.path.exists(os.path.join(self.workdirforgridpack, self.foldernameforrunpwg, "pwhg_main")):
      return 0, 1
    with cd(os.path.join(self.workdirforgridpack, self.foldernameforrunpwg)):
      for logfile in glob.iglob("run_*.log"):
        with open(logfile) as f:
          contents = f.read()
          if "Backtrace" in contents or "cannot load grid files" in contents:
            os.remove(logfile)
          if not contents.strip() and logfile.startswith("run_1_"):
            os.remove(logfile)
      for coredump in glob.iglob("core.*"):
        os.remove(coredump)
      for p, x in (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (2, 1), (3, 1):
        for n in range(1, 11):
          if not os.path.exists("run_{}_{}_{}.log".format(p, x, n)):
            return p, x
    return 9, 1

  def processmakegridpackstdout(self, stdout):
    if self.powhegsubmissionstrategy == "multicore":
      matches = [int(_) for _ in re.findall("Job <(.*)> is submitted to queue <.*>[.]", stdout)]
      for match in matches:
        with open(os.path.join(self.workdirforgridpack, "jobisrunning_{}".format(match)), 'w') as f:
          pass
    super(POWHEGMCSample, self).processmakegridpackstdout(stdout)
    #else don't need to do anything

  @property
  def makinggridpacksubmitsjob(self):
    if self.powhegsubmissionstrategy == "onestep":
      return "full_"+os.path.basename(self.powhegcard).replace(".input", "")
    elif self.powhegsubmissionstrategy == "multicore":
      if self.multicore_upto[0] in (0, 9):
        return False
      else:
        return True
    else:
      assert False, self.powhegsubmissionstrategy
  @property
  def gridpackjobsrunning(self):
    if self.powhegsubmissionstrategy == "multicore" and self.multicore_upto[0] in (1, 2, 3, 9):
      for filename in glob.iglob(os.path.join(self.workdirforgridpack, "jobisrunning_*")):
        jobid = int(os.path.basename(filename.replace("jobisrunning_", "")))
        if jobended(str(jobid)):
          os.remove(filename)
        else:
          return True
      return False
    return super(POWHEGMCSample, self).gridpackjobsrunning


  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit

    if self.powhegcardusesscript:
      powhegdir, powhegcard = os.path.split(self.powhegcard)
      powhegscript = os.path.join(powhegdir, "makecards.py")
      powhegscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, powhegscript.split("genproductions/")[-1])

      result = (       powhegscript + "\n"
              + "#    " + powhegcard)
    else:
      powhegcard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.powhegcard.split("genproductions/")[-1])
      result = powhegcard

    with cdtemp():
      if self.powhegcardusesscript:
        wget(powhegscript)
        wget(os.path.join(os.path.dirname(powhegscript), powhegcard.replace("M{}".format(self.mass), "template").replace("Wplus", "W").replace("Wminus", "W")))
        subprocess.check_call(["python", "makecards.py"])
      else:
        wget(powhegcard)
      with open(os.path.basename(powhegcard)) as f:
        powheggitcard = f.read()
        powheggitcardlines = [re.sub(" *([#!].*)?$", "", line) for line in powheggitcard.split("\n")]
        powheggitcardlines = [re.sub("(iseed|ncall2|fakevirt) *", r"\1 ", line) for line in powheggitcardlines
                              if line and all(_ not in line for _ in
                              ("pdfreweight", "storeinfo_rwgt", "withnegweights", "rwl_", "lhapdf6maxsets", "xgriditeration", "fakevirt")
                              )]
        powheggitcard = "\n".join(line for line in powheggitcardlines)

    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      if glob.glob("core.*") and self.cvmfstarball != "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/HJJ_M125_13TeV/HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_13TeV_M125.tgz":
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open("powheg.input") as f:
          powhegcard = f.read()
          powhegcardlines = [re.sub(" *([#!].*)?$", "", line) for line in powhegcard.split("\n")]
          powhegcardlines = [re.sub("(iseed|ncall2|fakevirt) *", r"\1 ", line) for line in powhegcardlines
                             if line and all(_ not in line for _ in
                             ("pdfreweight", "storeinfo_rwgt", "withnegweights", "rwl_", "lhapdf6maxsets", "xgriditeration", "fakevirt")
                             )]
          powhegcard = "\n".join(line for line in powhegcardlines)
      except IOError:
        raise ValueError("no powheg.input in the tarball\n{}".format(self))

    if powhegcard != powheggitcard:
      with cd(here):
        with open("powhegcard", "w") as f:
          f.write(powhegcard)
        with open("powheggitcard", "w") as f:
          f.write(powheggitcard)
      raise ValueError("powhegcard != powheggitcard\n{}\nSee ./powhegcard and ./powheggitcard".format(self))

    return result

  @property
  def productiongenerators(self):
    return ["powheg {}".format(self.powhegprocess)] + super(POWHEGMCSample, self).productiongenerators

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_powhegEmissionVeto_{:d}p_LHE_pythia8_cff.py".format(self.nfinalparticles)
  @abc.abstractproperty
  def nfinalparticles(self):
    pass

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "Powheg", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename

  def handle_request_fragment_check_caution(self, line):
    if line.strip() == "* [Caution: To check manually] This is a Powheg NLO sample. Please check 'nFinal' is":
      print "nFinal is", self.nfinalparticles
      return "ok"
    return super(POWHEGMCSample, self).handle_request_fragment_check_warning(line)

class AlternateWeight(collections.namedtuple("AlternateWeight", "lhapdf renscfact facscfact")):
  def __new__(cls, lhapdf, renscfact=None, facscfact=None):
    lhapdf = int(lhapdf)
    renscfact = cls.parsescalefactor(renscfact)
    facscfact = cls.parsescalefactor(facscfact)
    return super(AlternateWeight, cls).__new__(cls, lhapdf=lhapdf, renscfact=renscfact, facscfact=facscfact)

  @classmethod
  def parsescalefactor(cls, scalefactor):
    if scalefactor is None: return 1
    return float(scalefactor.replace("d", "e").replace("D", "E"))

  @property
  @cache
  def pdf(self):
    import lhapdf
    return lhapdf.mkPDF(self.lhapdf)

  @property
  def pdfname(self):
    return self.pdf.set().name
  @property
  def pdfmemberid(self):
    return self.pdf.memberID
