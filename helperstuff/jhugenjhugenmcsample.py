import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch

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
  def JHUGencardlocationintarball(self):
    return os.path.join(self.shortname+"_JHUGen", "JHUGen_decay.input")
