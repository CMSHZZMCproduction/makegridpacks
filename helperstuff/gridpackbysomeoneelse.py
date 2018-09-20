import abc, os, re, shutil

from utilities import cdtemp, mkdir_p, genproductions

import patches

from mcsamplebase import MCSampleBase, MCSampleBase_DefaultCampaign
from madgraphmcsample import MadGraphMCSample
from madgraphjhugenmcsample import MadGraphJHUGenMCSample

class GridpackBySomeoneElse(MCSampleBase):
  @property
  def defaulttimeperevent(self):
    return 60

  def createtarball(self):
    mkdir_p(os.path.dirname(self.foreostarball))
    if self.patchkwargs:
      kwargs = self.patchkwargs
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

  @property
  def makegridpackcommand(self):
    """
    if you implement this, you also HAVE to change tmptarball to be the correct name
    the directory doesn't matter, but the final filename should be whatever is created
    by the script
    """
    assert False
  @property
  def makinggridpacksubmitsjob(self):
    assert False

  @property
  def makegridpackscriptstolink(self):
    return ()

  @abc.abstractproperty
  def originaltarball(self):
    pass


class MadGraphGridpackBySomeoneElse(GridpackBySomeoneElse, MadGraphMCSample):
  pass

class MadGraphHZZdFromJake(MadGraphGridpackBySomeoneElse, MCSampleBase_DefaultCampaign):
  def __init__(self, year, Zdmass, eps):
    self.__Zdmass = int(str(Zdmass))
    self.__eps = float(eps)
    super(MadGraphHZZdFromJake, self).__init__(year=year)
  @property
  def identifiers(self):
    return "Jake", "HZZd", "madgraph", self.__Zdmass, self.__eps

  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    return v

  @property
  def originaltarball(self):
    return "/afs/cern.ch/work/d/drosenzw/public/HZZd4l_gridpacks/HAHM_variablesw_v3_MZd{}_eps{:.0e}_lhaid{}.tar.xz".format(self.__Zdmass, self.__eps, self.lhapdf).replace("e-0", "e-")
  @property
  def lhapdf(self):
    if self.year == 2016: return 263000
    if self.year == 2017: return 306000
    assert False, self

  @classmethod
  def allsamples(cls):
    for Zdmass in 20,:#1, 2, 3, 4, 7, 10, 15, 20, 25, 35:
      for eps in 1e-2,:
        for year in 2016, 2017:
          yield cls(year, Zdmass, eps)

  @property
  def generators(self):
    return "madgraph",
  @property
  def responsible(self):
    return "hroskes"

  def cvmfstarball_anyversion(self, version):
    if self.year == 2017: year = "2017"
    if self.year == 2016: year = "slc6_amd64_gcc481"
    tarballname = "ggH125_LO_HtoZZd_MZd{}_eps{:.0e}".format(self.__Zdmass, self.__eps)
    folder = os.path.join("/cvmfs/cms.cern.ch/phys_generator/gridpacks/", year, "13TeV/madgraph/V5_2.4.2/")
    return os.path.join(folder, tarballname, "v{}".format(version), tarballname+".tar.xz")
  @property
  def fragmentname(self):
    if self.year == 2017:
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_generic_LHE_pythia8_cff.py"
    if self.year == 2016:
      return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCUETP8M1_13TeV_generic_LHE_pythia8_cff.py"
    assert False, self.year
  @property
  def genproductionscommit(self):
    return "d61e214e3781a8cfec0f2d9b92f43d51638cd27a"
  @property
  def genproductionscommitforfragment(self):
    if self.year == 2018:
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
    if self.year == 2017:
      maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/2017/13TeV/Higgs/HToZZdTo4L_M125_MZd20_eps1e-2_13TeV_madgraph_pythia8")
    elif self.year == 2016:
      maindir = os.path.join(genproductions, "bin/MadGraph5_aMCatNLO/cards/production/pre2017/13TeV/Higgs/HToZZdTo4L_M125_MZd20_eps1e-2_13TeV_madgraph_pythia8")
    return (os.path.join(maindir, "makecards.sh"),) + tuple(os.path.join(maindir, "HAHMcards_eps_MZD_lhaid_template", os.path.basename(_)) for _ in self.madgraphcards)
  @property
  def madgraphcards(self):
    folder = "HAHMcards_eps{:.0e}_MZD{}_lhaid{}".format(self.__eps, self.__Zdmass, self.lhapdf).replace("e-02", "e-2")
    basenames = "HAHM_variablesw_v3_customizecards.dat", "HAHM_variablesw_v3_extramodels.dat", "HAHM_variablesw_v3_proc_card.dat", "HAHM_variablesw_v3_run_card.dat"
    return tuple(os.path.join(folder, basename) for basename in basenames)
  @property
  def datasetname(self):
    assert self.__eps == 1e-2
    return "HToZZdTo4L_M125_MZd{}_eps1e-2_13TeV_madgraph_pythia8".format(self.__Zdmass)

  @property
  def nevents(self):
    return 500000


class MadGraphHJJFromThomasPlusJHUGen(MadGraphGridpackBySomeoneElse, MadGraphJHUGenMCSample, MCSampleBase_DefaultCampaign):
  def __init__(self, year, coupling):
    self.__coupling = coupling
    super(MadGraphHJJFromThomasPlusJHUGen, self).__init__(year=year)
  @property
  def identifiers(self):
    return "Thomas", "HJJ", "madgraphJHUGen", self.__coupling

  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    if self.__coupling in ("a3", "a3mix") and self.year == 2017: v += 1  #tarball eventually used for HTT was different than the original one
    return v

  @property
  def patchkwargs(self):
    return {
      "functionname": "addJHUGentomadgraph",
      "JHUGenversion": "v7.1.4",
      "decaycard": self.decaycard,
    }

  @property
  def madgraphcards(self):
    assert False

  @property
  def decaycard(self):
    return os.path.join(genproductions, "bin", "JHUGen", "cards", "decay", "ZZ4l_notaus.input")

  @property
  def originaltarball(self):
    if self.__coupling == "SM":
        #from HIG-RunIIFall17wmLHEGS-01577
        return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/ggh012j_5f_NLO_FXFX_125/v2/ggh012j_5f_NLO_FXFX_125_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
    if self.__coupling == "a3":
        #from HIG-RunIIFall17wmLHEGS-01580
        #why is it in pre2017? no idea but it uses 306000
        return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc630/pre2017/13TeV/madgraph/v2.4.2/GluGluToMaxmixHToTauTau/v1/GluGluToMaxmixHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz"
    if self.__coupling == "a3mix":
        #from HIG-RunIIFall17wmLHEGS-01583
        #why is it in pre2017? no idea but it uses 306000
        return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/slc6_amd64_gcc630/pre2017/13TeV/madgraph/v2.4.2/GluGluToPseudoscalarHToTauTau/v1/GluGluToPseudoscalarHToTauTau_M125_13TeV_amcatnloFXFX_pythia8_slc6_amd64_gcc630_CMSSW_9_3_0_tarball.tar.xz"
    assert False, self

  @classmethod
  def allsamples(cls):
    for coupling in "SM", "a3", "a3mix":
      yield cls(2017, coupling)

  @property
  def generators(self):
    return "madgraph", "JHUGen v7.1.4"
  @property
  def responsible(self):
    return "hroskes"

  def cvmfstarball_anyversion(self, version):
    result = os.path.dirname(self.originaltarball)
    if re.match("v[0-9]*$", os.path.basename(result)): result = os.path.dirname(result)
    result += "_HZZ4l"
    result = os.path.join(result, "v{}".format(version), os.path.basename(self.originaltarball))
    return result

  @property
  def datasetname(self):
    if self.__coupling == "SM":
      return "GluGluHiggs0PMToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8"
    if self.__coupling == "a3":
      return "GluGluHiggs0MToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8"
    if self.__coupling == "a3mix":
      return "GluGluHiggs0Mf05ph0ToZZTo4L_M125_13TeV_amcatnloFXFX_JHUGenV714_pythia8"
  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_aMCatNLO_FXFX_5f_max2j_LHE_pythia8_cff.py"
  @property
  def genproductionscommit(self):
    if self.year == 2018:
      return "d2377ae8a03e2d36bdeb3255fc60761a9b247865"
    if self.year in (2016, 2017):
      return "d61e214e3781a8cfec0f2d9b92f43d51638cd27a"
  @property
  def hasfilter(self):
    assert False
  @property
  def xsec(self):
    assert False
  @property
  def tags(self):
    assert False
  @property
  def nevents(self):
    return 500000
