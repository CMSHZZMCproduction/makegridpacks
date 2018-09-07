import abc, os, re, shutil

from utilities import cdtemp, mkdir_p, genproductions

import patches

from mcsamplebase import MCSampleBase
from madgraphmcsample import MadGraphMCSample
from madgraphjhugenmcsample import MadGraphJHUGenMCSample

class GridpackBySomeoneElse(MCSampleBase):
  @property
  def nevents(self):
    assert False
  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    return v

  @property
  def datasetname(self):
    assert False
  @property
  def defaulttimeperevent(self):
    return 60
  @property
  def tags(self):
    assert False

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
  def tmptarball(self):
    assert False

  @property
  def makegridpackscriptstolink(self):
    return ()

  @abc.abstractproperty
  def originaltarball(self):
    pass


class MadGraphGridpackBySomeoneElse(GridpackBySomeoneElse, MadGraphMCSample):
  pass

class MadGraphHZZdFromLucien(MadGraphGridpackBySomeoneElse):
  def __init__(self, Zdmass, eps):
    self.__Zdmass = Zdmass
    self.__eps = eps
  @property
  def identifiers(self):
    return "Lucien", "HZZd", "madgraph", self.__Zdmass, self.__eps

  @property
  def originaltarball(self):
    return "/afs/cern.ch/work/d/drosenzw/public/HZZd4l_gridpacks/HAHM_variablesw_v3_MZd{}_eps{:e}.tar.xz".format(self.__Zdmass, self.__eps).replace("e-0", "e-")

  @classmethod
  def allsamples(cls):
    for Zdmass in 1, 2, 3, 4, 7, 10, 15, 20, 25, 35:
      for eps in 1e-2,:
        yield cls(Zdmass, eps)

  @property
  def generators(self):
    return "madgraph",
  @property
  def responsible(self):
    return "nobody for the moment"

  @property
  def cardsurl(self):
    assert False
  @property
  def cvmfstarball_anyversion(self):
    assert False
  @property
  def fragmentname(self):
    assert False
  @property
  def genproductionscommit(self):
    assert False
  @property
  def hasfilter(self):
    assert False
  @property
  def xsec(self):
    assert False

class MadGraphHJJFromThomasPlusJHUGen(MadGraphGridpackBySomeoneElse, MadGraphJHUGenMCSample):
  def __init__(self, coupling):
    self.__coupling = coupling
  @property
  def identifiers(self):
    return "Thomas", "HJJ", "madgraphJHUGen", self.__coupling

  @property
  def patchkwargs(self):
    return {
      "functionname": "addJHUGentomadgraph",
      "JHUGenversion": "v7.1.4",
      "decaycard": self.decaycard,
    }

  @property
  def decaycard(self):
    return os.path.join(genproductions, "bin", "JHUGen", "cards", "decay", "ZZ4l_notaus.input")

  @property
  def originaltarball(self):
    if self.__coupling == "SM":
        return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/ggh012j_5f_NLO_FXFX_125/v2/ggh012j_5f_NLO_FXFX_125_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
    if self.__coupling == "a3":
        return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/ggh012j_5f_NLO_FXFX_125_pseudoscalar/ggh012j_5f_NLO_FXFX_125_pseudoscalar_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
    if self.__coupling == "a3mix":
        return "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2/ggh012j_5f_NLO_FXFX_125_maxmix/ggh012j_5f_NLO_FXFX_125_maxmix_slc6_amd64_gcc481_CMSSW_7_1_30_tarball.tar.xz"
    assert False, self

  @classmethod
  def allsamples(cls):
    for coupling in "SM", "a3", "a3mix":
      yield cls(coupling)

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
  def cardsurl(self):
    assert False
  @property
  def fragmentname(self):
    assert False
  @property
  def genproductionscommit(self):
    assert False
  @property
  def hasfilter(self):
    assert False
  @property
  def xsec(self):
    assert False
