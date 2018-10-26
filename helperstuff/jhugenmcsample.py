import abc, glob, os, re
from helperstuff.utilities import genproductions
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
