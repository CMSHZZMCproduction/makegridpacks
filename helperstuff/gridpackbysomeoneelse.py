import abc, datetime, os, re, shutil

from utilities import cacheaslist, cdtemp, genproductions, KeepWhileOpenFile, mkdir_p

import patches

from mcsamplebase import MCSampleBase, Run2MCSampleBase, Run2UltraLegacyBase
from madgraphfxfxmcsample import MadGraphFXFXMCSample
from madgraphmcsample import MadGraphMCSample
from madgraphjhugenmcsample import MadGraphJHUGenMCSample

class GridpackBySomeoneElse(MCSampleBase):
  @property
  def defaulttimeperevent(self):
    return 60

  def createtarball(self):
    mkdir_p(self.workdirforgridpack)
    with KeepWhileOpenFile(self.tmptarball+".tmp") as kwof:
      if not kwof: return "another process is already copying the tarball"
      if not os.path.exists(self.originaltarball):
        return "original tarball does not exist"
      if datetime.datetime.fromtimestamp(os.path.getmtime(self.originaltarball)) <= self.modifiedafter:
        return "original tarball is an older version than we want"
      mkdir_p(os.path.dirname(self.foreostarball))
      if self.patchkwargs:
        kwargs = self.patchkwargs
        if isinstance(kwargs, list):
          kwargs = {"functionname": "multiplepatches", "listofkwargs": kwargs}
        for _ in "oldfilename", "newfilename", "sample": assert _ not in kwargs, _
        with cdtemp():
          kwargs["oldfilename"] = self.originaltarball
          kwargs["newfilename"] = os.path.abspath(os.path.basename(self.originaltarball))
          #kwargs["sample"] = self  #???
          patches.dopatch(**kwargs)
          shutil.move(os.path.basename(self.originaltarball), self.foreostarball)
      else:
        shutil.copy(self.originaltarball, self.foreostarball)
      return "gridpack is copied from "+self.originaltarball+" to this folder, to be copied to eos"

  @abc.abstractproperty
  def originaltarball(self):
    pass

  @property
  def modifiedafter(self):
    """
    if the original tarball was modified before this time, ignore it
    until it's replaced
    """
    return datetime.datetime(year=1900, month=1, day=1)


class MadGraphGridpackBySomeoneElse(GridpackBySomeoneElse, MadGraphMCSample):
  pass

class MadGraphHZZdFromJake(MadGraphGridpackBySomeoneElse):
  def __init__(self, year, VV, Zdmass, eps):
    self.__VV = VV
    self.__Zdmass = int(str(Zdmass))
    self.__eps = float(eps)
    super(MadGraphHZZdFromJake, self).__init__(year=year)
  @property
  def initargs(self): return self.year, self.__VV, self.__Zdmass, self.__eps
  @property
  def identifiers(self):
    return "Jake", "H"+self.__VV, "madgraph", self.__Zdmass, self.__eps

  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    if self.year in (2016, 2017, 2018) and self.__Zdmass == 20 and self.__eps == 1e-2 and self.__VV == "ZZd": v += 1 #comments on PR --> new tarball
    if self.year in (2016, 2017, 2018) and self.__VV == "ZdZd" and self.__eps == 2e-2 and (self.__Zdmass in (4, 7) or self.__Zdmass >= 10): v+=1
    return v

  @property
  def kap(self):
    if self.__VV == "ZdZd" and self.__eps == 2e-2: return 1e-4
    assert False, self

  @property
  def originaltarball(self):
    if self.__VV == "ZZd":
      return "/afs/cern.ch/work/d/drosenzw/public/HZZd_gridpacks/HAHM_variablesw_v3_eps{:.0e}_MZd{}_lhaid{}.tar.xz".format(self.__eps, self.__Zdmass, self.lhapdf).replace("e-0", "e-")
    if self.__VV == "ZdZd":
      return "/afs/cern.ch/work/d/drosenzw/public/HZdZd4l_gridpacks/HZdZd4l_lhapdf{lhapdf}_eps{eps:.0e}_kap{kap:.0e}/HAHM_variablesw_v3_eps{eps:.0e}_kap{kap:.0e}_mZd{Zdmass}_lhapdf{lhapdf}_slc6_amd64_gcc481_CMSSW_7_1_30.tar.xz".format(eps=self.__eps, Zdmass=self.__Zdmass, lhapdf=self.lhapdf, kap=self.kap).replace("e-0", "e-")
  @property
  def lhapdf(self):
    if self.__VV == "ZZd":
      if self.year == 2016: return 263000
      if self.year in (2017, 2018): return 306000
    if self.__VV == "ZdZd":
      return 306000
    assert False, self

  @property
  def responsible(self):
    return "hroskes"

  def cvmfstarball_anyversion(self, version):
    if self.year in (2017, 2018) or self.year == 2016 and self.__VV == "ZdZd": year = "2017"
    if self.year == 2016 and self.__VV == "ZZd": year = "slc6_amd64_gcc481"
    tarballname = "ggH125_LO_Hto"+self.__VV+"_MZd{}_eps{:.0e}".format(self.__Zdmass, self.__eps)
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/", year, "13TeV/madgraph/V5_2.4.2/")
    return os.path.join(folder, tarballname, "v{}".format(version), tarballname+".tar.xz")
  @property
  def fragmentname(self):
    if self.year in (2017, 2018):
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_generic_LHE_pythia8_cff.py"
    if self.year == 2016:
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCUETP8M1_13TeV_generic_LHE_pythia8_cff.py"
    assert False, self.year
  @property
  def genproductionscommit(self):
    if self.__VV == "ZZd":
      return "34b9e3dc408110faa10cf6317a0d901cd74e3ae1"
    if self.__VV == "ZdZd":
      return "6a37ef337c6cc5bf056e5b471af45ca85e1a8826"
    assert False, self
  @property
  def genproductionscommitforfragment(self):
    if self.__VV == "ZZd" and self.year == 2018:
      return "a93def45caca7548931ed014f933375828aaf8c8" #get the scale variations
    return super(MadGraphHZZdFromJake, self).genproductionscommitforfragment
  @property
  def hasfilter(self):
    return False
  @property
  def xsec(self):
    return 1 #unknown for unknown signal
  @property
  def tags(self):
    return ["HZZ"]

  @property
  def madgraphcardscript(self):
    if self.__VV == "ZZd":
      if self.year in (2017, 2018):
        maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/2017/13TeV/Higgs/HToZZdTo4L_M125_MZd20_eps1e-2_13TeV_madgraph_pythia8")
      elif self.year == 2016:
        maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/pre2017/13TeV/Higgs/HToZZdTo4L_M125_MZd20_eps1e-2_13TeV_madgraph_pythia8")
      subfolder = "HAHMcards_eps_MZD_lhaid_template"
    elif self.__VV == "ZdZd":
      maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/2017/13TeV/Higgs/HToZdZdTo4L_eps2e-2_13TeV_madgraph_pythia8/")
      subfolder = "HToZdZdcards_lhaid306000_eps2e-2_template"
    return (os.path.join(maindir, "makecards.sh"),) + tuple(os.path.join(maindir, subfolder, os.path.basename(_)) for _ in self.madgraphcards)
  @property
  def madgraphcards(self):
    if self.__VV == "ZZd":
      folder = "HAHMcards_eps{:.0e}_MZD{}_lhaid{}".format(self.__eps, self.__Zdmass, self.lhapdf).replace("e-02", "e-2")
    if self.__VV == "ZdZd":
      folder = "HToZdZdcards_eps{:.0e}_MZD{}_lhaid{}".format(self.__eps, self.__Zdmass, self.lhapdf).replace("e-02", "e-2")
    basenames = "HAHM_variablesw_v3_customizecards.dat", "HAHM_variablesw_v3_extramodels.dat", "HAHM_variablesw_v3_proc_card.dat", "HAHM_variablesw_v3_run_card.dat"
    return tuple(os.path.join(folder, basename) for basename in basenames)
  @property
  def datasetname(self):
    if self.__VV == "ZZd":
      assert self.__eps == 1e-2
      return "HTo"+self.__VV+"To4L_M125_MZd{}_eps1e-2_13TeV_madgraph_pythia8".format(self.__Zdmass)
    if self.__VV == "ZdZd":
      assert self.__eps == 2e-2 and self.kap == 1e-4
      return "HTo"+self.__VV+"To4L_M125_MZd{}_eps2e-2_kap1e-4_13TeV_madgraph_pythia8".format(self.__Zdmass)

  def comparecards(self, name, cardcontents, gitcardcontents):
    if self.__VV == "ZdZd":
      if name.endswith("_customizecards.dat"):
        cardcontents = cardcontents.replace("set param_card hidden 4 1.000000e-04", "set param_card hidden 4 1.000000e-09")
      if name.endswith("_proc_card.dat"):
        cardcontents = cardcontents.replace("zp", "Zp")
      if name.endswith("_run_card.dat"):
        cardcontents = gitcardcontents
    return super(MadGraphHZZdFromJake, self).comparecards(name, cardcontents, gitcardcontents)

  @property
  def nevents(self):
    return 100000

  @property
  def modifiedafter(self):
    """
    if the original tarball was modified before this time, ignore it
    until it's replaced
    """
    return datetime.datetime(year=2019, month=4, day=5)

class MadGraphHZZdFromJakeRun2(MadGraphHZZdFromJake, Run2MCSampleBase):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    for Zdmass in 1, 2, 3, 4, 7, 10, 15, 20, 25, 30, 35:
      for eps in 1e-2,:
        for year in 2016, 2017, 2018:
          for VV in "ZZd",:
            yield cls(year, VV, Zdmass, eps)

class MadGraphHZZdFromJakeRun2UL(MadGraphHZZdFromJake, Run2UltraLegacyBase):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    for Zdmass in 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60:
      for eps in 2e-2,:
        for year in 2016, 2017, 2018:
          for VV in "ZdZd",:
            yield cls(year, VV, Zdmass, eps)

class MadGraphHJJFromThomasPlusJHUGen(MadGraphGridpackBySomeoneElse, MadGraphJHUGenMCSample, MadGraphFXFXMCSample):
  def __init__(self, year, coupling, njets):
    self.__coupling = coupling
    self.__njets = njets
    super(MadGraphHJJFromThomasPlusJHUGen, self).__init__(year=year)

  @property
  def initargs(self): return self.year, self.__coupling, self.__njets

  @property
  def identifiers(self):
    return "Thomas", self.__njets, "madgraphJHUGen", self.__coupling

  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    if self.year in (2017, 2018) and self.__njets == "H012J": v += 1  #tarball eventually used for HTT was different than the original one for a3 and a3mix, and chmod u+x runcmsgrid.sh
    return v

  @property
  def patchkwargs(self):
    result = super(MadGraphHJJFromThomasPlusJHUGen, self).patchkwargs
    result.append({
      "functionname": "addJHUGentomadgraph",
      "JHUGenversion": self.JHUGenversion,
      "decaycard": self.decaycard,
    })
    return result

  @property
  def madgraphcardscript(self):
    if self.__coupling == "SM" and self.year in (2017, 2018) and self.__njets == "H012J":
      maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/2017/13TeV/Higgs/")
      script = os.path.join(maindir, "ggh012j_5f_NLO_FXFX.sh")
      cards = [os.path.join(maindir, "ggh012j_5f_NLO_FXFX_", os.path.basename(card).replace("125", "")) for card in self.madgraphcards + ["ggh012j_5f_NLO_FXFX_125_MadLoopParams.dat"]]
      return [script]+cards
  @property
  def madgraphcards(self):
    if self.year in (2017, 2018):
      if self.__njets == "H012J":
        if self.__coupling == "SM":
          return [
            os.path.join(
              "ggh012j_5f_NLO_FXFX_125",
              _,
            ) for _ in (
              "ggh012j_5f_NLO_FXFX_125_extramodels.dat",
              "ggh012j_5f_NLO_FXFX_125_customizecards.dat",
              "ggh012j_5f_NLO_FXFX_125_proc_card.dat",
              "ggh012j_5f_NLO_FXFX_125_run_card.dat",
            )
          ]

    couplingname = {
      "SM": "",
      "a3": "Pseudoscalar",
      "a3mix": "Maxmix",
    }[self.__coupling]
    yearfolder = {2016: "pre2017", 2017: "2017", 2018: "2017"}[self.year]
    njets = {"H012J": "012", "HJJ": "Two"}[self.__njets]
    njetsinfilename = {"H012J": "", "HJJ": "PlusTwoJets"}[self.__njets]
    folder = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/{yearfolder}/13TeV/Higgs/GluGluTo{coupling}HToTauTauPlus{njets}Jets_M125_13TeV_amcatnloFXFX_pythia8")
    return [
      os.path.join(folder, "GluGluTo{coupling}HToTauTau{njetsinfilename}_M125_13TeV_amcatnloFXFX_pythia8_{}.dat").format(_, coupling=couplingname, njets=njets, yearfolder=yearfolder, njetsinfilename=njetsinfilename)
        for _ in ("param_card", "proc_card", "run_card", "extramodels")
    ]
    assert False, self

  @property
  def decaycard(self):
    return os.path.join(genproductions, "bin", "JHUGen", "cards", "decay", "ZZ4l_notaus.input")

  @property
  def originaltarball(self):
    if self.__njets == "H012J":
      if self.year == 2016:
        if self.__coupling == "SM":
          #from HIG-RunIISummer15wmLHEGS-02181
          #why is it in 2017? no idea but it uses 292200
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTau_amcatnloFXFX/GluGluToHToTauTau_M125/v2/GluGluToHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3":
          #from HIG-RunIISummer15wmLHEGS-02187
          #why is it in 2017? no idea but it uses 292200
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTau_amcatnloFXFX/GluGluToPseudoscalarHToTauTau_M125/v2/GluGluToPseudoscalarHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3mix":
          #from HIG-RunIISummer15wmLHEGS-02184
          #why is it in 2017? no idea but it uses 292200
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTau_amcatnloFXFX/GluGluToMaxmixHToTauTau_M125/v2/GluGluToMaxmixHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
      if self.year in (2017, 2018):
        if self.__coupling == "SM":
          #from HIG-RunIIFall17wmLHEGS-01577
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/ggh012j_5f_NLO_FXFX_125/v2/ggh012j_5f_NLO_FXFX_125_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3":
          #from HIG-RunIIFall17wmLHEGS-01583
          #why is it in pre2017? no idea but it uses 306000
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc630/pre2017/13TeV/madgraph/v2.4.2/GluGluToPseudoscalarHToTauTau/v1/GluGluToPseudoscalarHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz"
        if self.__coupling == "a3mix":
          #from HIG-RunIIFall17wmLHEGS-01580
          #why is it in pre2017? no idea but it uses 306000
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc630/pre2017/13TeV/madgraph/v2.4.2/GluGluToMaxmixHToTauTau/v1/GluGluToMaxmixHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz"
    if self.__njets == "HJJ":
      if self.year == 2016:
        if self.__coupling == "SM":
          #from HIG-RunIISummer15wmLHEGS-02190
          #why is it in 2017? no idea but it uses 292200
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTau_amcatnloFXFX/GluGluToHToTauTauPlusTwoJets_M125/v2/GluGluToHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3":
          #from HIG-RunIISummer15wmLHEGS-02191
          #why is it in 2017? no idea but it uses 292200
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTau_amcatnloFXFX/GluGluToPseudoscalarHToTauTauPlusTwoJets_M125/v2/GluGluToPseudoscalarHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3mix":
          #from HIG-RunIISummer15wmLHEGS-02192
          #why is it in 2017? no idea but it uses 292200
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTau_amcatnloFXFX/GluGluToMaxmixHToTauTauPlusTwoJets_M125/v2/GluGluToMaxmixHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
      if self.year in (2017, 2018):
        if self.__coupling == "SM":
          #from HIG-RunIIFall17wmLHEGS-02398
          #yes, same as 2016 except v1 instead of v2 is correct :(.....
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8/v1/GluGluToHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3":
          #from HIG-RunIIFall17wmLHEGS-02421
          #yes, same as 2016 except v1 instead of v2 is correct :(.....
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToPseudoscalarHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8/v1/GluGluToPseudoscalarHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
        if self.__coupling == "a3mix":
          #from HIG-RunIIFall17wmLHEGS-02420
          #yes, same as 2016 except v1 instead of v2 is correct :(.....
          return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/GluGluToMaxmixHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8/v1/GluGluToMaxmixHToTauTauPlusTwoJets_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
    assert False, self

  @property
  def JHUGenversion(self): return "v7.1.4"
  @property
  def responsible(self):
    return "hroskes"

  def cvmfstarball_anyversion(self, version):
    if self.year == 2016:
      maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc481/13TeV/madgraph/V5_2.6.1/"
    elif self.year in (2017, 2018):
      if self.__njets == "H012J":
        maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/"
      if self.__njets == "HJJ":
        maindir = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.6.1/"

    if self.__njets == "H012J":
      folder = {
        "SM": "ggh012j_5f_NLO_FXFX_125_HZZ4l",
        "a3": "ggh012j_5f_NLO_FXFX_125_pseudoscalar_HZZ4l",
        "a3mix": "ggh012j_5f_NLO_FXFX_125_maxmix_HZZ4l",
      }[self.__coupling]
    elif self.__njets == "HJJ":
      folder = {
        "SM": "gghjj_5f_NLO_FXFX_125_HZZ4l",
        "a3": "gghjj_5f_NLO_FXFX_125_pseudoscalar_HZZ4l",
        "a3mix": "gghjj_5f_NLO_FXFX_125_maxmix_HZZ4l",
      }[self.__coupling]

    if self.__njets == "H012J":
      if self.year in (2017, 2018):
        if self.tarballversion >= 2:
          basename = {
            "SM": "ggh012j_5f_NLO_FXFX_JHUGenV714_HZZ4l_125_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
            "a3": "GluGluToMaxmixHToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz",
            "a3mix": "GluGluToPseudoscalarHToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz",
          }[self.__coupling]
        else:
          basename = folder.replace("HZZ4l", "slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz")
      elif self.year == 2016:
        basename = {
          "SM": "GluGluToHToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
          "a3": "GluGluToPseudoscalarHToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
          "a3mix": "GluGluToMaxmixHToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
        }[self.__coupling]
    elif self.__njets == "HJJ":
      basename = {
        "SM": "GluGluToHToZZTo4LPlusTwoJets_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
        "a3": "GluGluToPseudoscalarHToZZTo4LPlusTwoJets_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
        "a3mix": "GluGluToMaxmixHToZZTo4LPlusTwoJets_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz",
      }[self.__coupling]

    result = os.path.join(maindir, folder, "v{}".format(version), basename)
    return result

  @property
  def datasetname(self):
    if self.__njets == "HJJ": firstpart = "JJ"
    elif self.__njets == "H012J": firstpart = "GluGlu"

    if self.__coupling == "SM":
      return firstpart+"Higgs0PMToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8"
    if self.__coupling == "a3":
      return firstpart+"Higgs0MToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8"
    if self.__coupling == "a3mix":
      return firstpart+"Higgs0Mf05ph0ToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8"
  @property
  def nmaxjets(self): return 2
  @property
  def genproductionscommit(self):
    if self.__njets == "HJJ": return "d1f773cf0f2fb57c3bad824745eaac9a8958fc2d"
    if self.year == 2016:
      return "8245e42d267992a6d5f74898070eb82f2f2ca931"
    elif self.year in (2017, 2018):
      if self.__coupling == "SM":
        return "dce987f3bf6bd65c6d172b551b64209b241a8c1d"
      else:
        return "7d0525c9f6633a9ee00d4e79162d82e369250ccc"
  @property
  def genproductionscommitforfragment(self):
    if self.year == 2018:
      return "d2377ae8a03e2d36bdeb3255fc60761a9b247865"
    if self.year in (2016, 2017):
      return "d61e214e3781a8cfec0f2d9b92f43d51638cd27a"
  @property
  def hasnonJHUGenfilter(self):
    return True
  @property
  def xsec(self):
    return 1 #unknown for unknown signal
  @property
  def tags(self):
    return ["HZZ"]
  @property
  def nevents(self):
    if self.__njets == "H012J":
      return 500000
    elif self.__njets == "HJJ":
      return 3000000

class MadGraphHJJFromThomasPlusJHUGenRun2(MadGraphHJJFromThomasPlusJHUGen, Run2MCSampleBase):
  @classmethod
  def allsamples(cls):
    for year in 2016, 2017, 2018:
      for coupling in "SM", "a3", "a3mix":
        yield cls(year, coupling, "H012J")
        yield cls(year, coupling, "HJJ")

class MadgraphTWHPlusJHUGen(MadGraphGridpackBySomeoneElse, MadGraphJHUGenMCSample):
  def __init__(self, year, finalstate):
    self.finalstate = finalstate
    super(MadgraphTWHPlusJHUGen, self).__init__(year)
  @property
  def initargs(self):
    return self.year, self.finalstate
  @property
  def identifiers(self):
    return "tWH", "madgraphJHUGen", self.finalstate
  @property
  def JHUGenversion(self):
    return "v7.2.7"
  @property
  def decaycard(self):
    assert self.finalstate == "4l"
    return os.path.join(genproductions, "bin", "JHUGen", "cards", "decay", "ZZ2l2any_withtaus_filter4l.input")
  @property
  def responsible(self):
    return "hroskes"
  @property
  def tags(self):
    return ["HZZ"]
  @property
  def patchkwargs(self):
    result = super(MadgraphTWHPlusJHUGen, self).patchkwargs
    result.append({
      "functionname": "addJHUGentomadgraph",
      "JHUGenversion": self.JHUGenversion,
      "decaycard": self.decaycard,
    })
    return result
  @property
  def xsec(self):
    return 1 #unknown for unknown signal
  @property
  def fragmentname(self):
    if self.year in (2017, 2018):
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_generic_LHE_pythia8_cff.py"
  @property
  def datasetname(self):
    assert self.finalstate == "4l"
    return "TWH_HToZZ_4LFilter_M125_13TeV_madgraph_JHUGenV727_pythia8"
  def cvmfstarball_anyversion(self, version):
    assert self.finalstate == "4l"
    return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/thw_5f_ckm_LO_ctcvcp_MH125_JHUGen4l/v{}/thw_5f_ckm_LO_ctcvcp_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz".format(version)
  @property
  def genproductionscommit(self):
    return "04b3f0288d03f774eec6b4098877796cbd003b56"
  @property
  def hasnonJHUGenfilter(self):
    return False
  @property
  def originaltarball(self):
    return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/thw_5f_ckm_LO_ctcvcp_MH125/v1/thw_5f_ckm_LO_ctcvcp_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
  @property
  def tarballversion(self):
    v = 1
    return v
  @property
  def nevents(self):
    return 1000000
  @property
  def madgraphcards(self):
    return [
      os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/2017/13TeV/Higgs/thq_4f_ckm_LO_ctcvcp_MH", _)
      for _ in (
        "thq_4f_ckm_LO_ctcvcp_MH_customizecards.dat",
        "thq_4f_ckm_LO_ctcvcp_MH_proc_card.dat",
        "thq_4f_ckm_LO_ctcvcp_MH_run_card.dat",
        "thq_4f_ckm_LO_ctcvcp_MH_extramodels.dat",
        "thq_4f_ckm_LO_ctcvcp_MH_reweight_card.dat",
      )
    ]
  @property
  def madgraphcardscript(self):
    if self.year in (2017, 2018):
      maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/2017/13TeV/Higgs")
    return (os.path.join(maindir, "thw_5f_ckm_LO_ctcvcp.sh"),) + tuple(os.path.join(maindir, "thw_5f_ckm_LO_ctcvcp_MH", os.path.basename(_).replace("125", "")) for _ in self.madgraphcards)
  @property
  def madgraphcards(self):
    folder = "thw_5f_ckm_LO_ctcvcp_MH125"
    return tuple(os.path.join(folder, "thw_5f_ckm_LO_ctcvcp_MH125_" + _ + ".dat")
      for _ in ("customizecards", "proc_card", "run_card", "extramodels", "reweight_card"))
  @property
  def madgraphcardsrename(self):
    return "thw_5f_ckm_LO_ctcvcp_MH125", "thw_5f_ckm_LO_ctcvcp"

  def comparecards(self, name, cardcontents, gitcardcontents):
    if name.endswith("_customizecards.dat"):
      gitcardcontents = gitcardcontents.replace("set param_card mass 25 125\n", "")
    if name.endswith("_proc_card.dat"):
      cardcontents = cardcontents.replace("define ll = l+ l-\n", "")
      cardcontents = cardcontents.replace("define vll = vl vl~\n", "")
    return super(MadgraphTWHPlusJHUGen, self).comparecards(name, cardcontents, gitcardcontents)

class MadgraphTWHPlusJHUGenRun2(MadgraphTWHPlusJHUGen, Run2MCSampleBase):
  @classmethod
  def allsamples(cls):
    for year in 2016, 2017, 2018:
      if year == 2016: continue
      yield cls(year, "4l")
