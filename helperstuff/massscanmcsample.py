import abc, os

from utilities import genproductions

from mcsamplebase import MCSampleBase_DefaultCampaign

class MassScanMCSample(MCSampleBase_DefaultCampaign):
  def __init__(self, year, productionmode, decaymode, mass):
    self.productionmode = productionmode
    self.decaymode = decaymode
    self.mass = int(str(mass))
    super(MassScanMCSample, self).__init__(year=year)
  @property
  def initargs(self):
    return self.year, self.productionmode, self.decaymode, self.mass
  @property
  def identifiers(self):
    return self.productionmode, self.decaymode, self.mass
  @property
  def xsec(self): return 1 #unknown for unknown signal
  @abc.abstractproperty
  def reweightdecay(self): False
  @property
  def decaycard(self):
    folder = os.path.join(genproductions, "bin", "JHUGen", "cards", "decay")

    if self.decaymode == "4l":
      filename = "ZZ2l2any_withtaus_filter4l" if self.filter4L else "ZZ4l_withtaus"
      if self.reweightdecay: filename += "_reweightdecay_CPS"
      filename += ".input"
    elif self.decaymode == "2l2nu":
      if self.reweightdecay:
        filename = "ZZ2l2nu_notaus_reweightdecay_CPS.input"
    elif self.decaymode == "2l2q":
      if self.mass == 125:
        if self.productionmode == "ggH":
          filename = "ZZ2l2q_withtaus.input"
        elif self.productionmode in ("VBF", "WplusH", "WminusH", "bbH", "tqH"):
          filename = "ZZ2l2any_withtaus.input"
        elif self.productionmode in ("ZH", "ttH"):
          filename = "ZZany_filter2lOSSF.input"
        elif self.productionmode == "ggZH":
          filename = "ZZany_filter2l2jet.input"
      elif self.reweightdecay:
        filename = "ZZ2l2q_notaus_reweightdecay_CPS.input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card
