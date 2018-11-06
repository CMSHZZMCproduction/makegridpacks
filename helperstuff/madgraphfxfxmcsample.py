import abc

from madgraphmcsample import MadGraphMCSample

class MadGraphFXFXMCSample(MadGraphMCSample):
  @property
  def fragmentname(self):
    if self.year in (2017, 2018): tune = "CP5"
    if self.year == 2016: tune = "CUETP8M1"
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_Tune{}_13TeV_aMCatNLO_FXFX_5f_max{:d}j_LHE_pythia8_cff.py".format(tune, self.nmaxjets)
  @abc.abstractproperty
  def nmaxjets(self):
    pass
  def handle_request_fragment_check_caution(self, line):
    if line.strip() == "* [Caution: To check manually] This is a FxFx sample. Please check 'JetMatching:nJetMax' is set":
      print "nmaxjets is", self.nmaxjets
      return "ok"
    return super(MadgraphFXFXMCSample, self).handle_request_fragment_check_caution(line)

