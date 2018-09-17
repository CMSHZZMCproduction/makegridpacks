import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, scramarch, wget

from mcsamplebase import MCSampleBase, MCSampleBase_DefaultCampaign

class MadGraphMCSample(MCSampleBase):
  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "MadGraph5_aMCatNLO", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename
  def createtarball(self):
    return "making a madgraph tarball is not automated, you have to make it yourself and put it in {}".format(self.foreostarball)

  @property
  @cache
  def cardsurl(self):
    card = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", self.genproductionscommit, "Sumit fill this")

    with cdtemp():
      wget(card)
      with open(os.path.basename(card)) as f:
        gitcardcontents = f.read()
    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      if glob.glob("core.*") and self.cvmfstarball != "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/powheg/V2/HJJ_M125_13TeV/HJJ_slc6_amd64_gcc630_CMSSW_9_3_0_HJJ_NNPDF31_13TeV_M125.tgz":
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      cardnameintarball = "Sumit fill htis"
      try:
        with open(cardnameintarball) as f:
          cardcontents = f.read()
      except IOError:
        raise ValueError("no "+cardnameintarball+" in the tarball\n{}".format(self))

    if cardcontents != gitcardcontents:
      with cd(here):
        with open("cardcontents", "w") as f:
          f.write(cardcontents)
        with open("powheggitcard", "w") as f:
          f.write(gitcardcontents)
      raise ValueError("cardcontents != gitcardcontents\n{}\nSee ./cardcontents and ./gitcardcontents".format(self))

    return card

  @property
  def generators(self):
    return ["madgraph"]

class ZZ2L2QMadGraphMCSample(MadGraphMCSample, MCSampleBase_DefaultCampaign):
  def __init__(self, year, finalstate, cut=None):
    self.finalstate = finalstate
    self.cut = cut
    super(ZZ2L2QMadGraphMCSample, self).__init__(year=year)
  @property
  def identifiers(self):
    result = ["ZZ2L2Q", self.finalstate]
    if self.cut: result.append(self.cut)
    return tuple(result)
  @property
  def nevents(self):
    return 500000
  @property
  def hasfilter(self):
    return False

  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", str(self).replace(" ", ""), os.path.basename(self.cvmfstarball))
  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    return v

  def cvmfstarball_anyversion(self, version):
    folder = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/madgraph/V5_2.4.2"
    tarballname = self.datasetname + ".tgz"
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(version), tarballname)

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
  def datasetname(self):
     return "ZZTo2L2Q_13TeV_amcatnloFXFX_madspin_pythia8"

#  @property
#  def signalbkgbsitag(self):
#    if self.signalbkgbsi == 'SIG':      return ''
#    elif self.signalbkgbsi == 'BSI':    return 'Contin'
#    assert False

#  @property
#  def widthtag(self):
#    if self.width == 1: return ""
#    assert False, self.width

  @property
  def defaulttimeperevent(self):
    return 60
  @property
  def tags(self):
    return ["HZZ", "Fall17P2A"]
  @property
  def xsec(self):
    assert False, "need to fill this"

  @property
  def genproductionscommit(self): return "2e9528a99c5fabcc08a3eb33880e44b42ddaca70"
#    """
#    Sumit fill this
#    it has to be AFTER they merge your PR (because your PR has phantom stuff, but master has pythia stuff, so we need the merge)
#    """

  @classmethod
  def allsamples(cls):
     yield cls(2017, "2L2Q")

  @property
  def responsible(self):
    return "skeshri"

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_oneoversqrt2_LHE_pythia8_cff.py"
