import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

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
    return os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode)
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", self.foldernameforrunpwg,
             self.powhegprocess+"_"+scramarch+"_"+cmsswversion+"_"+self.foldernameforrunpwg+".tgz"
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
      elif self.multicore_upto in ((1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (2, 1), (3, 1)):
        args.update({
          "-p": str(self.multicore_upto[0]),
          "-x": str(self.multicore_upto[1]),
          "-j": "10",
          "-t": "100",
          "-n": "10",
          "-q": self.creategridpackqueue,
        })
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
    if self.powhegsubmissionstrategy == "multicore" and self.multicore_upto[0] != 0: return True
    return super(POWHEGMCSample, self).inthemiddleofmultistepgridpackcreation

  @property
  def multicore_upto(self):
    assert self.powhegsubmissionstrategy == "multicore", self.powhegsubmissionstrategy
    if not os.path.exists(os.path.join(self.workdir, self.foldernameforrunpwg)):
      return 0, 1
    for p, x in (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (2, 1), (3, 1):
      for n in range(1, 11):
        if not os.path.exists(os.path.join(
          self.workdir, self.foldernameforrunpwg, "run_{}_{}_{}.log".format(p, x, n)
        )):
          return p, x
    return 9, 1

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
    if self.powhegsubmissionstrategy == "multicore" and self.multicore_upto[0] in (1, 2, 3):
      p, x = self.multicore_upto
      for n in range(1, 11):
        if not os.path.exists(os.path.join(
          self.workdir, self.foldernameforrunpwg, "run_{}_{}_{}.log".format(p, x, n)
        )) and os.path.exists(os.path.join(
          self.workdir, self.foldernameforrunpwg, "run_{}_{}_{}.sh".format(p, x, n)
        )) and not jobended("-J", "{}_{}_{}".format(p, x, n)):
          #In other words, if there's a job with this name currently running,
          #AND the .sh exists, AND the log doesn't exist, we can't conclude that
          #the job isn't running
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
      subprocess.check_output(["tar", "xvzf", self.cvmfstarball])
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
  def generators(self):
    return ["powheg {}".format(self.powhegprocess)]

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_powhegEmissionVeto_{:d}p_LHE_pythia8_cff.py".format(self.nfinalparticles)

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "Powheg", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename

