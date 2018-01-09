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

    if self.decaymode == "4l":
        if self.productionmode == "ZH":
            filename = "ZZ2l2any_withtaus_filter4l.input"
        if self.productionmode == "ttH":
            filename = "ZZ2l2any_withtaus_filter4lOSSF.input"
        else:
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
