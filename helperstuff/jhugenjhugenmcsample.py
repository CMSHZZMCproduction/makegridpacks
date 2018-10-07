import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from helperstuff.mcsamplebase import MCSampleBase
from jhugenmcsample import JHUGenMCSample
from jhugendecaymcsample import JHUGenDecayMCSample

class JHUGenJHUGenMCSample(JHUGenMCSample, JHUGenDecayMCSample):
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir",self.productionmode+"_"+self.decaymode, os.path.basename(self.productioncard).replace(".input", ""),
             "JHUGen_"+self.shortname+"_"+scramarch+"_"+cmsswversion+".tgz")
  @property
  def shortname(self):
    return re.sub(r"\W", "", str(self)).replace(str(self.year), "", 1)
  @property
  def makegridpackcommand(self):
    return super(JHUGenJHUGenMCSample, self).makegridpackcommand + ["--decay-card", self.decaycard]
  @property
  def makinggridpacksubmitsjob(self):
    return None

  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit
    decaycard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.decaycard.split("genproductions/")[-1])
    if self.productioncardusesscript:
      productiondir, productioncard = os.path.split(self.productioncard)
      productionscript = os.path.join(productiondir, "makecards.py")
      productionscript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, productionscript.split("genproductions/")[-1])

      result = (       productionscript + "\n"
              + "#    " + productioncard + "\n"
              + "# " + decaycard)
    else:
      productioncard = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.productioncard.split("genproductions/")[-1])
      result = (       productioncard + "\n"
              + "# " + decaycard)

    with cdtemp():
      if self.productioncardusesscript:
        wget(productionscript)
        wget(os.path.join(os.path.dirname(productionscript), productioncard.replace("M{}".format(self.mass), "template").replace("Wplus", "W").replace("Wminus", "W")))
        subprocess.check_call(["python", "makecards.py"])
      else:
        wget(productioncard)
      with open(os.path.basename(productioncard)) as f:
        productiongitcard = f.read()
      with contextlib.closing(urllib.urlopen(decaycard)) as f:
        decaygitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      for root, dirnames, filenames in os.walk('.'):
        for filename in filenames:
          if re.match("core[.].*", filename):
            raise ValueError("There is a core dump in the tarball\n{}".format(self))
      try:
        with open(os.path.join(self.shortname+"_JHUGen", "JHUGen.input")) as f:
          productioncard = f.read()
      except IOError:
        raise ValueError("no JHUGen.input in the tarball\n{}\n{}".format(self, self.cvmfstarball))
      try:
        with open(os.path.join(self.shortname+"_JHUGen", "JHUGen_decay.input")) as f:
          decaycard = f.read()
      except IOError:
        raise ValueError("no JHUGen_decay.input in the tarball\n{}".format(self))

    if productioncard != productiongitcard:
      with cd(here):
        with open("productioncard", "w") as f:
          f.write(productioncard)
        with open("productiongitcard", "w") as f:
          f.write(productiongitcard)
      raise ValueError("productioncard != productiongitcard\n{}\n{}\n{}".format(self, productioncard, productiongitcard))
    if decaycard != decaygitcard:
      raise ValueError("decaycard != decaygitcard\n{}\n{}\n{}".format(self, decaycard, decaygitcard))

    return result
