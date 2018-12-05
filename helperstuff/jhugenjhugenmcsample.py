import abc, contextlib, glob, os, re, subprocess

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
    #accomodate either before or after https://github.com/cms-sw/genproductions/commit/c745241379a09d78f8ec63b3c468ccfeffa8e88b#diff-05dd22003a895cdee21435db7fa1800c
    folder = self.shortname+"_JHUGen"
    if not os.path.exists(folder): folder = "."

    return os.path.join(folder, "JHUGen_decay.input")
