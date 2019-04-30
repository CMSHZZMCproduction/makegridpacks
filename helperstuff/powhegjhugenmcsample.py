import abc, contextlib, glob, os, re, subprocess

from utilities import cache, cd, cdtemp, genproductions, here, makecards, wget

from powhegmcsample import POWHEGMCSample
from jhugendecaymcsample import JHUGenDecayMCSample

class POWHEGJHUGenMCSample(POWHEGMCSample, JHUGenDecayMCSample):
  @property
  def makegridpackcommand(self):
    return super(POWHEGJHUGenMCSample, self).makegridpackcommand + ["-g", self.decaycard]
  @property
  def foldernameforrunpwg(self):
    return super(POWHEGJHUGenMCSample, self).foldernameforrunpwg+"_"+self.decaymode

  @property
  def JHUGencardlocationintarball(self):
    return "JHUGen.input"

  @property
  def JHUGenlocationintarball(self):
    return "./JHUGen"
