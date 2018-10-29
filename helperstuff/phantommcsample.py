import abc, contextlib, glob, os, re, subprocess, urllib

import uncertainties

from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, mkdir_p, scramarch, wget

from mcsamplebase import MCSampleBase_DefaultCampaign
from mcsamplewithxsec import MCSampleWithXsec

class PhantomMCSample(MCSampleBase_DefaultCampaign, MCSampleWithXsec):
  def __init__(self, year, signalbkgbsi, finalstate, mass, width):
    self.signalbkgbsi = signalbkgbsi
    self.finalstate = finalstate
    self.mass = mass
    self.width = width
    super(PhantomMCSample, self).__init__(year=year)
  @property
  def identifiers(self):
    return self.signalbkgbsi, "PHANTOM", self.mass, self.width, self.finalstate
  @property
  def nevents(self):
    if self.finalstate in ("2e2mu" ,"4e", "4mu"):
      return 500000
    if self.finalstate in ("2e2nue" ,"2e2num", "2e2nut","2mu2nue","2mu2num","2mu2nut"):
      return 250000
  @property
  def hasfilter(self):
    return False
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", str(self).replace(" ", ""), os.path.basename(self.cvmfstarball))
  @property
  def tarballversion(self):
    v = 1

#    if self.finalstate == "2mu2num" and self.signalbkgbsi == "BSI" : v+=1
#    if self.finalstate == "2e2nue" and self.signalbkgbsi == "BSI" : v+=1
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

  def getxsec(self):
    dats = set(glob.iglob("result"))
    if len(dats) != 1:
      raise ValueError("Expected to find result in the tarball {}\n".foramt(self.cvmfstarball))
    with open(dats.pop()) as f:
      matches = re.findall(r"total cross section=\s*([0-9.Ee+-]*)\s*[+]/-\s*([0-9.Ee+-]*)\s*", f.read())
    if not matches: raise ValueError("Didn't find the cross section in the result\n\n"+self.cvmfstarball)
    if len(matches) > 1: raise ValueError("Found multiple cross section lines in the result\n\n")
    xsec, xsecerror = matches[0]
    return uncertainties.ufloat(xsec, xsecerror)

  @property
  def genproductionscommit(self):
    return "59eab4505ac61b2fcd677d82c15aa8d6d0ced28f"

  @classmethod
  def allsamples(cls):
    for signalbkgbsi in ["SIG", "BSI", "BKG"]:
      for finalstate in ["2e2mu","4e","4mu","2e2nue","2e2num","2e2nut","2mu2nue","2mu2num","2mu2nut"]:
        for mass in 125,:
          for width in 1,:
            yield cls(2017, signalbkgbsi, finalstate, mass, width)

  @property
  def responsible(self):
    return "hroskes"

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
  def cardsurl(self):

    icard = "VBF_"
    if self.signalbkgbsi == "SIG" or self.signalbkgbsi == "BSI": icard += "H125"
    if self.signalbkgbsi == "BKG" or self.signalbkgbsi == "BSI": icard += "ZZcont"
    icard += "_NNPDF31_13TeV_"
    icard += {
      "4e": "ee_ee_",
      "4mu": "mumu_mumu_",
      "2e2mu": "ee_mumu_",
      "2e2nue": "ee_veve_",
      "2e2num": "ee_vmvm_",
      "2e2nut": "ee_vtvt_",
      "2mu2nue": "mumu_veve_",
      "2mu2num": "mumu_vmvm_",
      "2mu2nut": "mumu_vtvt_",
    }[self.finalstate]
    icard += ".py"

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

    moreresult = super(PhantomMCSample, self).cardsurl
    if moreresult: card += "\n# " + moreresult

    return card

  @property
  def productiongenerators(self):
    return ["phantom"]

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_pTmaxFudge_oneoversqrt2_LHE_pythia8_cff.py"
