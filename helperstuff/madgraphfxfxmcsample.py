import abc

from madgraphmcsample import MadGraphMCSample

class MadGraphFXFXMCSample(MadGraphMCSample):
  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_aMCatNLO_FXFX_5f_max{:d}j_LHE_pythia8_cff.py".format(self.nmaxjets)
  @abc.abstractproperty
  def nmaxjets(self):
    pass
  def handle_request_fragment_check_warning(self, line):
    if line.strip() == "* [Caution: To check manunally] This is a FxFx sample. Please check 'JetMatching:nJetMax' is set":
      print "nmaxjets is", self.nmaxjets
      return "ok"
    return super(MadgraphFXFXMCSample, self).handle_request_fragment_check_warning(line)

