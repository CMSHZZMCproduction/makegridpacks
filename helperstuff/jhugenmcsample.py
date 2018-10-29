import abc, glob, os, re, subprocess
from helperstuff.utilities import cdtemp, genproductions, wget
from helperstuff.mcsamplebase import MCSampleBase

class UsesJHUGenLibraries(MCSampleBase):
  @property
  def JHUGenversion(self):
    return 
  @property
  def productiongenerators(self):
    assert re.match(r"v[0-9]+[.][0-9]+[.][0-9]+", self.JHUGenversion), self.JHUGenversion
    return super(UsesJHUGenLibraries, self).productiongenerators + ["JHUGen {}".format(self.JHUGenversion)]

class JHUGenMCSample(UsesJHUGenLibraries):
  @abc.abstractproperty
  def productioncard(self): pass
  @abc.abstractproperty
  def productioncardusesscript(self):
    pass
  @abc.abstractproperty
  def JHUGenversion(self):
    pass
  @property
  def linkmela(self): return False
  @property
  def makegridpackcommand(self):
    args = {
      "--card": self.productioncard,
      "--name": self.shortname,
      "-n": "10",
      "-s": str(hash(self) % 2147483647),
    }
    if self.linkmela: args["--link-mela"] = None
    return ["./install.py"] + sum(([k] if v is None else [k, v] for k, v in args.iteritems()), [])
  @property
  def makinggridpacksubmitsjob(self):
    return None
  @abc.abstractproperty
  def shortname(self): pass

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "JHUGen", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename

  @property
  def cardsurl(self):
    commit = self.genproductionscommit
    if self.productioncardusesscript:
      productiondir, productioncard = os.path.split(self.productioncard)
      productionscript = os.path.join(productiondir, "makecards.py")
      productionscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, productionscript.split("genproductions/")[-1])

      result = (       productionscript + "\n"
              + "#    " + productioncard)
    else:
      productioncard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.productioncard.split("genproductions/")[-1])
      result = productioncard

    with cdtemp():
      if self.productioncardusesscript:
        wget(productionscript)
        wget(os.path.join(os.path.dirname(productionscript), productioncard.replace("M{}".format(self.mass), "template").replace("Wplus", "W").replace("Wminus", "W")))
        subprocess.check_call(["python", "makecards.py"])
      else:
        wget(productioncard)
      with open(os.path.basename(productioncard)) as f:
        productiongitcard = f.read()

    try:
      with open(os.path.join(self.shortname+"_JHUGen", "JHUGen.input")) as f:
        productioncard = f.read()
    except IOError:
      raise ValueError("no JHUGen.input in the tarball\n{}\n{}".format(self, self.cvmfstarball))

    if productioncard != productiongitcard:
      with cd(here):
        with open("productioncard", "w") as f:
          f.write(productioncard)
        with open("productiongitcard", "w") as f:
          f.write(productiongitcard)
      raise ValueError("productioncard != productiongitcard\n{}\n{}\n{}".format(self, productioncard, productiongitcard))

    moreresult = super(JHUGenMCSample, self).cardsurl
    if moreresult: result += "\n# "+moreresult

    return result
