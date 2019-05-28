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
  def handle_request_fragment_check_warning(self, line):
    match = re.match(r"\* \[WARNING\] To check manually - This is a matched MadGraph LO sample\. Please check 'JetMatching:nJetMax' =\s*(\d+)\s*is OK and", line.strip())
    if match:
      if self.nmaxjets == int(match.group(1)):
        print "nmaxjets is", self.nmaxjets
        return "ok"
      else:
        return "Number of jets is not set correctly (?)"
    return super(MadgraphFXFXMCSample, self).handle_request_fragment_check_warning(line)

class MadGraphMCSampleNoJets(MadGraphMCSample):
  def handle_request_fragment_check_warning(self, line):
    if line == "* [WARNING] To check manually - This is a matched MadGraph LO sample. Please check 'JetMatching:nJetMax' =100 is OK and":
      return "ok as per https://hypernews.cern.ch/HyperNews/CMS/get/prep-ops/6138.html"
    return super(MadgraphFXFXMCSample, self).handle_request_fragment_check_warning(line)

