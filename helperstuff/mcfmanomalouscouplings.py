import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, makecards

from mcfmmcsample import MCFMMCSample

class MCFMAnomCoupMCSample(MCFMMCSample):
  def __init__(self, signalbkgbsi, width, coupling, finalstate):
    self.signalbkgbsi = signalbkgbsi
    self.width = int(str(width))
    self.coupling = coupling
    self.finalstate = finalstate
  @property
  def identifiers(self):
    return self.signalbkgbsi, self.width, self.coupling, self.finalstate
  @property
  def nevents(self):
    return 5000000

  @property
  def keepoutput(self):
    return False

  @property
  def widthtag(self):
    if int(self.width) == 1:
	return ''
    else:
	return str(self.width)

  @property
  def productioncard(self):
    folder = os.path.join('genproductions','bin','MCFM','cards','MCFM+JHUGen',self.signalbkgbsi)
    cardbase = 'MCFM_JHUGen_13TeV_ggZZto{finalstate}_{sigbkgbsi}{widthtag}_NNPDF31.DAT'.format(finalstate=self.finalstate,sigbkgbsi=self.signalbkgbsi,widthtag=self.widthtag)
    card = os.path.join(folder,cardbase)
    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def hasfilter(self):
    return False

  @property
  def queue(self):
    return "2nd"

  @property
  def tarballversion(self):
    v = 1

    return v

  @property
  def cvmfstarball(self): 
    folder = os.path.join('genproduction','bin','MCFM')
    tarballname = self.datasetname+".tgz"
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(self.tarballversion), tarballname)

  @property
  def datasetname(self):
   return 'GluGluToHiggs{coupling}{signalbkgbsitag}ToZZTo{finalstatetag}_M125_{widthtag}GaSM_13TeV_MCFM701_pythia8'.format(coupling=self.coupling,finalstatetag=self.datasetfinalstatetag,widthtag=self.widthtag,signalbkgbsitag=self.signalbkgbsitag) 

  @property 
  def signalbkgbsitag(self):
    if self.signalbkgbsi == 'SIG':	return ''
    elif self.signalbkgbsi == 'BSI':	return 'contin'

  @property 
  def datasetfinalstatetag(self):
    states = []
    tag = ''
    p1,p2 = self.finalstate[:2], self.finalstate[2:] 
    if p1 == p2:
	if p1 == 'EL':		return '4e'
	elif p1 == 'MU':	return '4mu'
	else:			return '4tau'
    else:
	for p in p1,p2:
		if p == 'EL':	tag += '2e'
		elif p == 'MU': tag += '2mu'
		elif p == 'TL': tag += '2tau'
		else p == 'NU': tag += '2nu'
	return tag
    
  @property
  def defaulttimeperevent(self):
    return 30
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P3"]

  @property
  def genproductionscommit(self):
    return "30f2b0996446b94cf97165a40ab4b296550afc2e"

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"

  @classmethod
  def getcouplings(cls, signalbkgbsi):
    if signalbkgbsi in ("SIG", "BSI"): return "0PM", "0PH", "0PHf05ph0", "0PL1", "0PL1f05ph0", "0M", "0Mf05ph0"
    assert False, signalbkgbsi

  @classmethod
  def getwidths(cls, signalbkgbsi, coupling):
    if signalbkgbsi == "SIG": return 1,
    if signalbkgbsi == "BSI":
      if coupling == "SM": return 1, 10, 25
      return 1, 10

  @classmethod
  def allsamples(cls):
    for signalbkgbsi in "SIG", "BSI":
      for finalstate in "ELEL", "ELMU", "ELTL", "ELNU", "MUMU","MUNU","TLTL":
        for coupling in cls.getcouplings(signalbkgbsi):
          for width in cls.getwidths(signalbkgbsi, coupling):
            yield cls(signalbkgbsi, width, coupling, finalstate)

  @property
  def responsible(self):
     return "wahung"
