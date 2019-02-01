#!/usr/bin/env python

import os

from jhugenmcsample import JHUGenMCSample
from mcsamplebase import MCSampleBase_DefaultCampaign
from mcsamplewithxsec import MCSampleWithXsec_RunZeroEvents

class JHUGenOffshellVBF(JHUGenMCSample, MCSampleBase_DefaultCampaign, MCSampleWithXsec_RunZeroEvents):
  def __init__(self, year, signalbkgbsi, width, coupling, finalstate):
    self.signalbkgbsi = signalbkgbsi
    self.width = width
    self.coupling = coupling
    self.finalstate = finalstate
    super(JHUGenOffshellVBF, self).__init__(year=year)

  @property
  def identifiers(self):
    return "offshellVBF", self.signalbkgbsi, self.width, self.coupling, self.finalstate

  @property
  def nevents(self):
    return 4000000

  @property
  def makegridpackcommand(self):
    result = super(JHUGenOffshellVBF, self).makegridpackcommand
    result.append("--vbf-offshell")
    if self.inthemiddleofmultistepgridpackcreation: result.append("--check-jobs")
    return result

  @property
  def inthemiddleofmultistepgridpackcreation(self):
    if os.path.exists(self.tmptarball): return False
    if os.path.exists(os.path.join(self.workdirforgridpack, self.shortname+"_JHUGen")): return True
    return False

  @classmethod
  def allsamples(cls):
    for year in 2016, 2017, 2018:
      if year == 2016: continue
      for finalstate in "4l", "2l2nu":
        for signalbkgbsi in "signal", "bkg", "BSI":
          for coupling in "SM", "a3", "a3mix", "a2", "a2mix", "L1", "L1mix":
            for width in "GaSM", "10GaSM":
              if signalbkgbsi == "bkg" and coupling != "SM": continue
              if width == "10GaSM" and signalbkgbsi != "BSI": continue
              yield cls(year, signalbkgbsi, width, coupling, finalstate)

  @property
  def JHUGenversion(self):
    return "v7.2.7"
  
  def cvmfstarball_anyversion(self, version):
    if self.year in (2017, 2018):
      folder = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/jhugen/V727/VBF_offshell_ZZ_NNPDF31_13TeV"
    tarballname = self.datasetname+".tgz"
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(version), tarballname)

  @property
  def datasetname(self):
   repmap = {
     "finalstate": self.finalstate,
     "coupling": self.couplingfordatasetname,
     "Contin?": "Contin" if self.signalbkgbsi != "signal" else "",
     "widthtag": self.width,
   }
   if self.signalbkgbsi == "bkg":
     return "VBFToContinToZZTo{finalstate}_13TeV_MCFM701_pythia8".format(**repmap)
   return "VBFToHiggs{coupling}{Contin?}ToZZTo{finalstate}_M125_{widthtag}_13TeV_MCFM701_pythia8".format(**repmap)

  @property
  def defaulttimeperevent(self):
    return 100  #no idea

  @property
  def fragmentname(self):
    if self.year in (2017, 2018):
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_half_LHE_pythia8_cff.py"
    assert False, self
  @property
  def genproductionscommit(self):
    return "18bd285551f4166be7217f1f48302074561ef3e7"
  @property
  def genproductionscommitforfragment(self):
    if self.year == 2017: return "fd7d34a91c3160348fd0446ded445fa28f555e09"
    return super(JHUGenOffshellVBF, self).genproductionscommitforfragment

  @property
  def hasfilter(self): return False
  @property
  def productioncard(self):
    card = "{signalbkgbsi}{width}ZZ{finalstate}_withtaus{coupling}.input".format(
      signalbkgbsi=self.signalbkgbsi,
      width={"10GaSM": "10", "GaSM": ""}[self.width],
      finalstate=self.finalstate,
      coupling=("_"+self.coupling if self.signalbkgbsi != "bkg" else ""),
    )

    return os.path.join(genproductions, "bin/JHUGen/cards/2017/13TeV/VBFoffshell/", card)
  @property
  def productioncardusesscript(self):
    return True
  @property
  def responsible(self):
    return "hroskes"
  @property
  def tags(self):
    return "HZZ", "HIG"
  @property
  def tarballversion(self):
    v = 1
    return v

  @property
  def couplingfordatasetname(self):
    return {
      "SM": "0PM",
      "a3": "0M",
      "a3mix": "0Mf05ph0",
      "a2": "0PH",
      "a2mix": "0PHf05ph0",
      "L1": "0L1",
      "L1mix": "0L1f05ph0",
    }[self.coupling]
