import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from mcfmmcsample import MCFMMCSample

class MCFMAnomCoupMCSample(MCFMMCSample):
  def __init__(self, signalbkgbsi, width, coupling):
    self.signalbkgbsi = signalbkgbsi
    self.width = int(str(width))
    self.coupling = coupling
  @property
  def identifiers(self):
    return self.signalbkgbsi, self.width, self.coupling
  @property
  def nevents(self):
    "Carol fill in"
  @property
  def keepoutput(self):
    return False

  @property
  def productioncard(self):
    "Carol fill in"
#   something like:
#    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "2017", "13TeV", "anomalouscouplings", self.productionmode+"_NNPDF31_13TeV")
#    cardbase = self.productionmode
#    card = os.path.join(folder, self.kind + ".input")

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def hasfilter(self):
    return False

  @property
  def queue(self):
    return "1nd"

  @property
  def tarballversion(self):
    v = 1

    return v

  @property
  def cvmfstarball(self):
    "Carol fill in"
    folder = os.path.join("...")

    tarballname = self.datasetname+".tgz"

    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(self.tarballversion), tarballname)

  @property
  def datasetname(self):
    "Carol fill in"

  @property
  def defaulttimeperevent(self):
    return 30
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P3"]

  @property
  def genproductionscommit(self):
    "Carol change this after you make the cards"
    return "441e6efc2cba80560477251ac06aaba1d60253e6"

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_generic_LHE_pythia8_cff.py"

  @classmethod
  def getcouplings(cls, signalbkgbsi):
    if signalbkgbsi in ("Sig", "BSI"): return "SM", "a2", "a3", "L1", "L1Zg", "a2mix", "a3mix", "L1mix", "L1Zgmix"
    assert False, signalbkgbsi

  @classmethod
  def getwidths(cls, signalbkgbsi, coupling):
    if signalbkgbsi == "Sig": return 1,
    if signalbkgbsi == "BSI":
      if coupling == "SM": return 1, 10, 25
      return 1, 10

  @classmethod
  def allsamples(cls):
    for signalbkgbsi in "Sig", "BSI":
      for coupling in cls.getcouplings(signalbkgbsi):
        for width in cls.getwidths(signalbkgbsi, coupling):
          yield cls(signalbkgbsi, width, coupling)

  @property
  def responsible(self):
     return "wahung"
