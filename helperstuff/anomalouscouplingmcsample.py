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
    return self.productionmode, self.decaymode, self.mass
  @abc.abstractproperty
  def reweightdecay(self): False
  @property
  def decaycard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

#    if self.decaymode == "4l":
#      filename = "ZZ2l2any_withtaus_filter4l" if self.filter4L else "ZZ4l_withtaus"
#      if self.reweightdecay: filename += "_reweightdecay_CPS"
#      filename += ".input"
#    elif self.decaymode == "2l2nu":
#      if self.reweightdecay:
#        filename = "ZZ2l2nu_notaus_reweightdecay_CPS.input"

    if self.decaymode == "4l":
	filename = "ZZ4l_withtaus.input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def nevents(self):
    if self.decaymode == "4l":
      if self.productionmode == "HJJ":
        return 50000
#      elif self.productionmode in ("VBF", "ZH", "ttH", "bbH"):
#        if 124 <= self.mass <= 126 or self.mass >= 1500: return 500000
#        return 200000

    raise ValueError("No nevents for {}".format(self))

  @property
  def keepoutput(self):
    return False
