import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from mcsamplebase import MCSampleBase

class POWHEGJHUGenMCSample(MCSampleBase):
  @abc.abstractproperty
  def powhegprocess(self): pass
  @abc.abstractproperty
  def powhegcard(self): pass
  @abc.abstractproperty
  def decaycard(self): pass
  @abc.abstractproperty
  def powhegcardusesscript(self): pass
  @property
  def hasfilter(self): return "filter" in self.decaycard.lower()
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode),
             self.powhegprocess+"_"+scramarch+"_"+cmsswversion+"_"+os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode+".tgz"))
  @property
  def makegridpackcommand(self):
    args = {
      "-p": "f",
      "-i": self.powhegcard,
      "-g": self.decaycard,
      "-m": self.powhegprocess,
      "-f": os.path.basename(self.powhegcard).replace(".input", "_"+self.decaymode),
      "-q": self.queue,
      "-n": "10",
      "-s": str(hash(self) % 2147483647),
    }
    return ["./run_pwg.py"] + sum(([k, v] for k, v in args.iteritems()), [])
  @property
  def makinggridpacksubmitsjob(self):
    return "full_"+os.path.basename(self.powhegcard).replace(".input", "")

  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])

    if self.powhegcardusesscript:
      powhegdir, powhegcard = os.path.split(self.powhegcard)
      powhegscript = os.path.join(powhegdir, "makecards.py")
      powhegscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, powhegscript.split("genproductions/")[-1])

      result = (       powhegscript + "\n"
              + "#    " + powhegcard + "\n"
              + "# " + JHUGencard)
    else:
      powhegcard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.powhegcard.split("genproductions/")[-1])
      result = (       powhegcard + "\n"
              + "# " + JHUGencard)

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
      with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
        JHUGengitcard = f.read()

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

  @property
  def generators(self):
    return ["powheg {}".format(self.powhegprocess), "JHUGen v7.0.11"]

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_powhegEmissionVeto_{:d}p_LHE_pythia8_cff.py".format(self.nfinalparticles)

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "Powheg", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename
