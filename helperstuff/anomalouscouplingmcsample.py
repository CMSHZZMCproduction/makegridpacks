import abc, os

from utilities import genproductions

from mcsamplebase import MCSampleBase

class AnomalousCouplingMCSample(MCSampleBase):
  def __init__(self, productionmode, decaymode, mass,kind):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))
    self.kind = kind
  @property
  def identifiers(self):
    return self.productionmode, self.decaymode, self.mass, self.kind
  @property
  def decaycard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

    couplings = ["L1","L1Zg","L1Zgmix","L1mix","a2","a2mix","a3","a3mix"]

    if self.decaymode == "4l":
        if self.productionmode == "VBF" or self.productionmode == "WH":
            for coupling in couplings :
                if coupling in self.kind and not "mix" in coupling :
                   filename = "anomalouscouplings/ZZ4l_withtaus_" +coupling+".input"
                else :
                   filename = "anomalouscouplings/ZZ4l_withtaus_" +coupling+"for"+self.productionmode+".input"
        if self.productionmode == "ZH" or self.productionmode == "HJJ":
            for coupling in couplings :
                if coupling in self.kind :
                   filename = "anomalouscouplings/ZZ4l_withtaus_" +coupling+".input"
        if self.productionmode == "ttH":
            filename = "ZZ4l_withtaus.input"
        if self.productionmode == "VBF" or self.productionmode == "ZH" or self.productionmode == "WH" or self.productionmode == "HJJ" and "SM" in self.kind :
            filename = "ZZ4l_withtaus.input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def nevents(self):
    if self.decaymode == "4l":
      if self.productionmode in ("HJJ", "ttH"):
        return 250000
      elif self.productionmode in ("ggH", "VBF", "WH"):
        return 500000
      elif self.productionmode == "ZH":
        return 750000

    raise ValueError("No nevents for {}".format(self))

  @property
  def keepoutput(self):
    return False
