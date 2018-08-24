import abc, os, shutil

from utilities import mkdir_p

from mcsamplebase import MCSampleBase

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


class MadgraphGridpackBySomeoneElse(GridpackBySomeoneElse):
  pass

class MadgraphHZZdFromLucien(MadgraphGridpackBySomeoneElse):
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
    return "hroskes"

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
