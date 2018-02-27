import contextlib, csv, os, re, subprocess, urllib

from utilities import cache, cd, genproductions, here, makecards

from mcfmmcsample import MCFMMCSample

class MCFMAnomCoupMCSample(MCFMMCSample):
  def __init__(self, signalbkgbsi, width, coupling, finalstate):
    self.signalbkgbsi = signalbkgbsi
    self.width = int(width)
    self.coupling = coupling
    self.finalstate = finalstate
  @property
  def identifiers(self):
    return self.signalbkgbsi, self.width, self.coupling, self.finalstate
  @property
  def nevents(self):
    if self.finalstate=='ELEL' or self.finalstate=='MUMU':  return 1000000
    else:  return 500000

  @property
  def extensionnumber(self):
    result = super(MCFMAnomCoupMCSample, self).extensionnumber
    if self.signalbkgbsi == "BKG": result += 1
    return result

  @property
  def widthtag(self):
    if int(self.width) == 1:
	return ''
    else:
	return str(self.width)

  @property
  def productioncard(self):
    folder = os.path.join(genproductions,'bin','MCFM','cards','MCFM+JHUGen')
    if self.signalbkgbsi in ("SIG", "BSI"):
      folder = os.path.join(folder,self.signalbkgbsi)
    cardbase = 'MCFM_JHUGen_13TeV_ggZZto{finalstate}_{sigbkgbsi}{widthtag}_NNPDF31.DAT'.format(finalstate=self.finalstate,sigbkgbsi=self.signalbkgbsi,widthtag=self.widthtag)
    card = os.path.join(folder,cardbase)
    if not os.path.exists(card):
      raise IOError(card+" does not exist")
    return card

  @property
  def hasfilter(self):
    return False

  @property
  def creategridpackqueue(self):
    return "2nd"


  @property
  def tarballversion(self):
    v = 2
#    if self.signalbkgbsi == "BKG": v
    identifierstr = ' '.join(map(str,self.identifiers))
    with cd(here), open('listofv2tarballs.txt','r') as f:
	if identifierstr in f.read():  v+=1   
#    if self.signalbkgbsi == 'BSI' and self.finalstate == 'ELMU' and self.coupling == '0M':  v+=1
    return v

  @property
  def cvmfstarball(self): 
    folder = '/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/mcfm/'   
    if self.signalbkgbsi == "BKG":
        mainfoldername = "MCFM_mdata_MCFM_JHUGen_13TeV_ggZZto{}_BKG_NNPDF31".format(self.finalstate)
        tarballname = "MCFM_mdata_slc6_amd64_gcc630_CMSSW_9_3_0_MCFM_JHUGen_13TeV_ggZZto{}_BKG_NNPDF31.tgz".format(self.finalstate)
    else:
        tarballname = self.tmptarball.split('/')[-1]
        mainfoldername = tarballname.replace(".tgz", "")
    return os.path.join(folder, mainfoldername, "v{}".format(self.tarballversion), tarballname)

  @property
  def datasetname(self):
   if self.signalbkgbsi == "BKG":
     return "GluGluToContinToZZTo{finalstatetag}_13TeV_MCFM701_pythia8".format(finalstatetag=self.datasetfinalstatetag)
   return 'GluGluToHiggs{coupling}{signalbkgbsitag}ToZZTo{finalstatetag}_M125_{widthtag}GaSM_13TeV_MCFM701_pythia8'.format(coupling=self.coupling,finalstatetag=self.datasetfinalstatetag,widthtag=self.widthtag,signalbkgbsitag=self.signalbkgbsitag)

  @property 
  def signalbkgbsitag(self):
    if self.signalbkgbsi == 'SIG':	return ''
    elif self.signalbkgbsi == 'BSI':	return 'contin'
    assert False

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
		elif p == 'NU': tag += '2nu'
	return tag
    
  @property
  def defaulttimeperevent(self):
    return 50
    assert False

  @property
  def tags(self):
    return ["HZZ", "Fall17P3"]

  @property
  def genproductionscommit(self):
#    return "ae49ee695eb68125cff65b0d47eab2e859c3e9aa"
    return "bcee2b23ce664c6dac4adaaac259dfdb9834493d"

  @property
  def fragmentname(self):
    return "Configuration/GenProduction/python/ThirteenTeV/Hadronizer/Hadronizer_TuneCP5_13TeV_pTmaxMatch_1_LHE_pythia8_cff.py"

  @classmethod
  def getcouplings(cls, signalbkgbsi):
    if signalbkgbsi in ("SIG", "BSI"): return ["0PM", "0PH", "0PHf05ph0", "0PL1", "0PL1f05ph0", "0M", "0Mf05ph0"]
    if signalbkgbsi == "BKG": return ["0PM"]
    assert False, signalbkgbsi

  @classmethod
  def getwidths(cls, signalbkgbsi, coupling):
    if signalbkgbsi in ("SIG", "BKG"): return 1,
    if signalbkgbsi == "BSI":
      if coupling == "0PM": return 1, 10
      return 1, 10

  @classmethod
  def allsamples(cls):
    for signalbkgbsi in [ "BSI","SIG", "BKG"]:
      for finalstate in ["ELTL",'MUTL','ELMU',"ELNU","MUMU","MUNU","TLTL","ELEL"]:
        for coupling in cls.getcouplings(signalbkgbsi):
          for width in cls.getwidths(signalbkgbsi, coupling):
            yield cls(signalbkgbsi, width, coupling, finalstate)

  @property
  def responsible(self):
     return "wahung"
