import abc, contextlib, glob, os, re, subprocess

from utilities import cache, cd, cdtemp, genproductions, here, makecards, wget

from jhugendecaymcsample import JHUGenDecayMCSample
from madgraphmcsample import MadGraphMCSample

class MadGraphJHUGenMCSample(MadGraphMCSample, JHUGenDecayMCSample):
  @property
  def makegridpackcommand(self): assert False, self
  @property
  def otherthingsininputcards(self): return ["InputCards/JHUGen.input"]

  @property
  def JHUGencardlocationintarball(self):
    return "InputCards/JHUGen.input"

  @property
  def JHUGenlocationintarball(self):
    return "./JHUGen"
