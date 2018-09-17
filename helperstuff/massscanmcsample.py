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
      elif self.reweightdecay:
        filename = "ZZ2l2q_notaus_reweightdecay_CPS.input"

    card = os.path.join(folder, filename)

    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def nevents(self):
    if self.decaymode == "4l":
      if self.productionmode == "ggH":
        if 124 <= self.mass <= 126: return 1000000
        return 500000
      elif self.productionmode in ("VBF", "ZH", "ttH", "bbH"):
        if 124 <= self.mass <= 126 or self.mass >= 1500: return 500000
        return 200000
      elif self.productionmode == "WplusH":
        if 124 <= self.mass <= 126: return 300000
        return 180000
      elif self.productionmode == "WminusH":
        if 124 <= self.mass <= 126: return 200000
        return 120000
      elif self.productionmode == "tqH":
        if self.mass == 125: return 1000000
    elif self.decaymode == "2l2nu":
      if self.productionmode in ("ggH", "VBF"):
        if 200 <= self.mass <= 1000: return 250000
        elif self.mass > 1000: return 500000
    elif self.decaymode == "2l2q":
      if self.productionmode == "ggH":
        if self.mass == 125: return 1000000
        elif 200 <= self.mass <= 1000: return 200000
        elif self.mass > 1000: return 500000
      elif self.productionmode == "VBF":
        if self.mass == 125: return 500000
        elif 200 <= self.mass <= 1000: return 100000
        elif self.mass > 1000: return 500000
      elif self.productionmode in ("ZH", "ttH", "bbH", "tqH"):
        if self.mass == 125: return 500000
      elif self.productionmode == "WplusH":
        if self.mass == 125: return 300000
      elif self.productionmode == "WminusH":
        if self.mass == 125: return 200000

    raise ValueError("No nevents for {}".format(self))
