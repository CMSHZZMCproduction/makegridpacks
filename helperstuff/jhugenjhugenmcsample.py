import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from mcsamplebase import MCSampleBase

class JHUGenJHUGenMCSample(MCSampleBase):
  @abc.abstractproperty
  def productioncard(self): pass
  @property
  def hasfilter(self): return "filter" in self.decaycard.lower()
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", os.path.basename(self.productioncard).replace(".input", "_"+self.decaymode),
             "JHUGen_"+self.shortname+"_"+scramarch+"_"+cmsswversion+".tgz")
  @property
  def shortname(self):
    return re.sub(r"\W", "", str(self))
  @property
  def linkmela(self): return False
  @property
  def makegridpackcommand(self):
    args = {
      "--card": self.productioncard,
      "--decay-card": self.decaycard,
      "--name": self.shortname,
      "-n": "10",
      "-s": str(hash(self) % 2147483647),
    }
    if self.linkmela: args["--link-mela"] = None
    return ["./install.py"] + sum(([k] if v is None else [k, v] for k, v in args.iteritems()), [])
  @property
  def makinggridpacksubmitsjob(self):
    return None

  @property
  @cache
  def cardsurl(self):
    productiondir, productioncard = os.path.split(self.productioncard)
    productionscript = os.path.join(productiondir, "makecards.py")
    commit = self.genproductionscommit
    productionscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, productionscript.split("genproductions/")[-1])
    JHUGencard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])

    result = (       productionscript + "\n"
            + "#    " + productioncard + "\n"
            + "# " + JHUGencard)

    with cdtemp():
      wget(productionscript)
      wget(os.path.join(os.path.dirname(productionscript), productioncard.replace("M{}".format(self.mass), "template").replace("Wplus", "W").replace("Wminus", "W")))
      subprocess.check_call(["python", "makecards.py"])
      with open(productioncard) as f:
        productiongitcard = f.read()
        productiongitcardlines = [re.sub(" *([#!].*)?$", "", line) for line in productiongitcard.split("\n")]
        productiongitcardlines = [re.sub("(iseed|ncall2|fakevirt) *", r"\1 ", line) for line in productiongitcardlines
                              if line and all(_ not in line for _ in
                              ("pdfreweight", "storeinfo_rwgt", "withnegweights", "rwl_", "lhapdf6maxsets", "xgriditeration")
                              )]
        productiongitcard = "\n".join(line for line in productiongitcardlines)
      with contextlib.closing(urllib.urlopen(JHUGencard)) as f:
        JHUGengitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvzf", self.cvmfstarball])
      if glob.glob("core.*"):
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open("JHUGen.input") as f:
          productioncard = f.read()
          productioncardlines = [re.sub(" *([#!].*)?$", "", line) for line in productioncard.split("\n")]
          productioncardlines = [re.sub("(iseed|ncall2|fakevirt) *", r"\1 ", line) for line in productioncardlines
                             if line and all(_ not in line for _ in
                             ("pdfreweight", "storeinfo_rwgt", "withnegweights", "rwl_", "lhapdf6maxsets", "xgriditeration")
                             )]
          productioncard = "\n".join(line for line in productioncardlines)
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}".format(self))
      try:
        with open("JHUGen.input") as f:
          JHUGencard = f.read()
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}".format(self))

    if productioncard != productiongitcard:
      with cd(here):
        with open("productioncard", "w") as f:
          f.write(productioncard)
        with open("productiongitcard", "w") as f:
          f.write(productiongitcard)
      raise ValueError("productioncard != productiongitcard\n{}\nSee ./productioncard and ./productiongitcard".format(self))
    if JHUGencard != JHUGengitcard:
      raise ValueError("JHUGencard != JHUGengitcard\n{}\n{}\n{}".format(self, JHUGencard, JHUGengitcard))

    return result

  @property
  def generators(self):
    return ["JHUGen v7.0.11"]

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_generic_LHE_pythia8_cff.py"

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "JHUGen", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename
