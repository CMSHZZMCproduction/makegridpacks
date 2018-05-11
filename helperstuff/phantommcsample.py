import abc, contextlib, glob, os, re, subprocess, urllib

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, mkdir_p, scramarch, wget

from mcsamplebase import MCSampleBase

class PhantomMCSample(MCSampleBase):
  def __init__(self, signalbkgbsi, finalstate, mass, width):
    self.signalbkgbsi = signalbkgbsi
    self.finalstate = finalstate
    self.mass = mass
    self.width = width
  @property
  def identifiers(self):
    return self.signalbkgbsi, "PHANTOM", self.mass, self.width, self.finalstate
  @property
  def nevents(self):
    if self.finalstate in ("2e2mu" ,"4e", "4mu"):
      return 500000
    if self.finalstate in ("2e2nue" ,"2e2num", "2e2nut","2mu2nue","2mu2num","2mu2nut"):
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
    folder = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/phantom"
    tarballname = self.datasetname + ".tar.xz"
    return os.path.join(folder, tarballname.replace(".tar.xz", ""), "v{}".format(version), tarballname)

  @property
  def datasetname(self):
   if self.signalbkgbsi == "BKG":
     return "VBFToContinToZZTo{finalstate}JJ_13TeV_phantom_pythia8".format(finalstate=self.finalstate)
   return 'VBFToHiggs0PM{signalbkgbsitag}ToZZTo{finalstate}JJ_M{mass}_{widthtag}GaSM_13TeV_phantom_pythia8'.format(finalstate=self.finalstate,widthtag=self.widthtag,signalbkgbsitag=self.signalbkgbsitag,mass=self.mass)

  @property
  def signalbkgbsitag(self):
    if self.signalbkgbsi == 'SIG':      return ''
    elif self.signalbkgbsi == 'BSI':    return 'Contin'
    assert False

  @property
  def widthtag(self):
    if self.width == 1: return ""
    assert False, self.width

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
  def genproductionscommit(self):
    return "76336e9844b0df3aaf1496a40755ca772beecbb6"
    """
####    Sumit fill this
    it has to be AFTER they merge your PR (because your PR has phantom stuff, but master has pythia stuff, so we need the merge)
    """

  @classmethod
  def allsamples(cls):
    for signalbkgbsi in ["SIG", "BSI", "BKG"]:
      for finalstate in ["2e2mu","4e","4mu","2e2nue","2e2num","2e2nut","2mu2nue","2mu2num","2mu2nut"]:
        for mass in 125,:
          for width in 1,:
            yield cls(signalbkgbsi, finalstate, mass, width)

  @property
  def responsible(self):
    return "skeshri"

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
    for filename in glob.iglob(os.path.join(genproductions, "bin", "Phantom", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename
  def createtarball(self):
    mkdir_p(os.path.dirname(self.foreostarball))
    return "making a phantom tarball is not automated, you have to make it yourself and put it in {}".format(self.foreostarball)

  @property
  @cache
  def cardsurl(self):
   cards=['VBF_H125_NNPDF31_13TeV_ee_ee_.py','VBF_H125ZZcont_NNPDF31_13TeV_ee_ee_.py','VBF_ZZcont_NNPDF31_13TeV_ee_ee_.py','VBF_H125_NNPDF31_13TeV_ee_mumu_.py','VBF_H125ZZcont_NNPDF31_13TeV_ee_mumu_.py', 'VBF_ZZcont_NNPDF31_13TeV_ee_mumu_.py','VBF_H125_NNPDF31_13TeV_ee_veve_.py','VBF_H125ZZcont_NNPDF31_13TeV_ee_veve_.py','VBF_ZZcont_NNPDF31_13TeV_ee_veve_.py','VBF_H125_NNPDF31_13TeV_ee_vmvm_.py','VBF_H125ZZcont_NNPDF31_13TeV_ee_vmvm_.py','VBF_ZZcont_NNPDF31_13TeV_ee_vmvm_.py','VBF_H125_NNPDF31_13TeV_ee_vtvt_.py','VBF_H125ZZcont_NNPDF31_13TeV_ee_vtvt_.py','VBF_ZZcont_NNPDF31_13TeV_ee_vtvt_.py','VBF_H125_NNPDF31_13TeV_mumu_mumu_.py','VBF_H125ZZcont_NNPDF31_13TeV_mumu_mumu_.py','VBF_ZZcont_NNPDF31_13TeV_mumu_mumu_.py','VBF_H125_NNPDF31_13TeV_mumu_veve_.py','VBF_H125ZZcont_NNPDF31_13TeV_mumu_veve_.py','VBF_ZZcont_NNPDF31_13TeV_mumu_veve_.py','VBF_H125_NNPDF31_13TeV_mumu_vmvm_.py','VBF_H125ZZcont_NNPDF31_13TeV_mumu_vmvm_.py','VBF_ZZcont_NNPDF31_13TeV_mumu_vmvm_.py','VBF_H125_NNPDF31_13TeV_mumu_vtvt_.py',  'VBF_H125ZZcont_NNPDF31_13TeV_mumu_vtvt_.py','VBF_ZZcont_NNPDF31_13TeV_mumu_vtvt_.py']

   for icard in cards :
    card = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", self.genproductionscommit, "bin/Phantom/cards/production/13TeV/HZZ_VBFoffshell_Phantom",icard)

    with cdtemp():
      wget(card)
      with open(os.path.basename(card)) as f:
        gitcardcontents = f.read()
    with cdtemp():
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      if glob.glob("core.*"):
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
      cardnameintarball = icard
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
    return ["phantom"]

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_oneoversqrt2_LHE_pythia8_cff.py"
