import contextlib, csv, os, re, subprocess

from utilities import cache, cacheaslist, cd, genproductions, here, makecards

from mcfmmcsample import MCFMMCSample
from mcsamplebase import MCSampleBase, Run2MCSampleBase, Run2UltraLegacyBase

class MCFMAnomCoupMCSample(MCFMMCSample):
  def __init__(self, year, signalbkgbsi, width, coupling, finalstate):
    self.signalbkgbsi = signalbkgbsi
    self.width = int(width)
    self.coupling = coupling
    self.finalstate = finalstate
    super(MCFMAnomCoupMCSample, self).__init__(year=year)
  @property
  def initargs(self): return self.year, self.signalbkgbsi, self.width, self.coupling, self.finalstate
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
    if self.year == 2017:
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
    return "testmatch"


  @property
  def tarballversion(self):
    v = 1
    if self.year in (2017, 2018):
      v += 1
#      if self.signalbkgbsi == "BKG": v
      identifierstr = ' '.join(map(str,self.identifiers))
      if 'BSI' in identifierstr and '0PL1f05ph0 TLTL' in identifierstr:  v+=1 
      if 'BSI' in identifierstr and '0PL1f05ph0 ELEL' in identifierstr: v+=1 
      if 'BSI 1 0PL1f05ph0 MUMU' == identifierstr: v+=2 
      with cd(here), open('data/listofv2tarballs.txt','r') as f:
        if identifierstr in f.read():  v+=1   
#      if self.signalbkgbsi == 'BSI' and self.finalstate == 'ELMU' and self.coupling == '0M':  v+=1
      if 'BSI 1 0PL1f05ph0 ELEL' == identifierstr: v=7
      if 'BSI 1 0PL1f05ph0 MUMU' == identifierstr: v=6
      if 'BSI 1 0PL1f05ph0 TLTL' == identifierstr: v=8
      if 'BSI 10 0Mf05ph0 TLTL' == identifierstr: v=4
      if 'BSI 10 0Mf05ph0 ELEL' == identifierstr: v=4
      if 'BSI 10 0Mf05ph0 MUMU' == identifierstr: v=4 
      with cd(here), open('data/listofpatchedmcfmgridpacks.txt', 'r') as f:
        if identifierstr in f.read():  v+=1

    if self.year == 2018 and self.signalbkgbsi in ("SIG", "BSI"):
      v+=1  #increase ncalls1 and 2

    if self.year == 2018 and identifierstr == "BSI 10 0PH ELMU": v+=1
    if self.year == 2018 and identifierstr == "BSI 1 0M ELMU": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PM MUMU": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PHf05ph0 MUMU": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PL1f05ph0 TLTL": v+=1
    if self.year == 2018 and identifierstr == "BSI 1 0M TLTL": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PH ELEL": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PHf05ph0 ELEL": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0Mf05ph0 ELEL": v+=1

    if self.year == 2018 and identifierstr == "BSI 10 0PM MUMU": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PHf05ph0 MUMU": v+=1
    if self.year == 2018 and identifierstr == "BSI 1 0M TLTL": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PH ELEL": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0PHf05ph0 ELEL": v+=1
    if self.year == 2018 and identifierstr == "BSI 10 0Mf05ph0 ELEL": v+=1

    if self.year == 2018 and self.signalbkgbsi in ("SIG", "BSI"):
      v+=1  #csmax patch

    return v

  def cvmfstarball_anyversion(self, version):
    folder = '/cvmfs/cms.cern.ch/phys_generator/gridpacks/2017/13TeV/mcfm/'   
    if self.signalbkgbsi == "BKG":
        mainfoldername = "MCFM_mdata_MCFM_JHUGen_13TeV_ggZZto{}_BKG_NNPDF31".format(self.finalstate)
        tarballname = "MCFM_mdata_slc6_amd64_gcc630_CMSSW_9_3_0_MCFM_JHUGen_13TeV_ggZZto{}_BKG_NNPDF31.tgz".format(self.finalstate)
    else:
        tarballname = self.tmptarballbasename
        mainfoldername = tarballname.replace(".tgz", "")
    return os.path.join(folder, mainfoldername, "v{}".format(version), tarballname)

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
    result = ["HZZ"]
    if self.year == 2017:
      result.append("Fall17P3")
    return result

  @property
  def genproductionscommit(self):
    if self.year == 2018 and self.signalbkgbsi != "BKG":
      return "32f097d84517f364edf7e86630ce5646b12b835e"
    return "138efefa8acdcc246a0df4512bef3f660574cb77"

  @property
  def genproductionscommitforfragment(self):
    if self.year == 2018: return "20f59357146e08e48132cfd73d0fd72ca08b6b30"
    return super(MCFMAnomCoupMCSample, self).genproductionscommitforfragment

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

  @property
  def responsible(self):
     if self.signalbkgbsi != "BKG" or "TL" in self.finalstate or "NU" in self.finalstate: return "nobody"
     return "hroskes"

  @property
  def maxallowedtimeperevent(self):
    return 175

  @property
  def tweaktimepereventseed(self):
    if self.year == 2018 and self.signalbkgbsi == "BKG" and self.finalstate in "ELEL MUMU": return 1
    return super(MCFMAnomCoupMCSample, self).tweaktimepereventseed
  @property
  def tweakmakegridpackseed(self):
    result = super(MCFMAnomCoupMCSample, self).tweakmakegridpackseed
    if self.year in (2017, 2018) and self.signalbkgbsi == "BSI" and self.width == 10 and self.finalstate in ("ELEL MUMU TLTL") and self.coupling == "0PH": result += 1
    if self.year in (2017, 2018) and self.signalbkgbsi == "BSI" and self.width == 10 and self.finalstate in ("ELEL MUMU TLTL") and self.coupling == "0PM": result += 1
    if self.year in (2017, 2018) and self.signalbkgbsi == "BSI" and self.width == 10 and self.finalstate in ("ELEL MUMU TLTL") and self.coupling == "0PHf05ph0": result += 1
    if self.year in (2017, 2018) and self.signalbkgbsi == "BSI" and self.width == 1 and self.finalstate in ("ELEL MUMU TLTL") and self.coupling == "0M": result += 1
    if self.year in (2017, 2018) and self.signalbkgbsi == "BSI" and self.width == 10 and self.finalstate in ("ELEL MUMU TLTL") and self.coupling == "0PH": result += 1
    if self.year in (2017, 2018) and self.signalbkgbsi == "BSI" and self.width == 10 and self.finalstate in ("ELEL MUMU TLTL") and self.coupling == "0Mf05ph0": result += 1
    return result

  @property
  def neventsfortest(self):
    return 1000
    return super(MCFMAnomCoupMCSample, self).neventsfortest

  @property
  def cmsswversion(self):
    return "CMSSW_9_3_0"
  @property
  def scramarch(self):
    return "slc6_amd64_gcc630"

class MCFMAnomCoupMCSampleRun2(MCFMAnomCoupMCSample, Run2MCSampleBase):
  @classmethod
  @cacheaslist
  def allsamples(cls):
    for signalbkgbsi in [ "BSI","SIG", "BKG"]:
      for finalstate in ["ELTL",'MUTL','ELMU',"ELNU","MUMU","MUNU","TLTL","ELEL"]:
        for coupling in cls.getcouplings(signalbkgbsi):
          for width in cls.getwidths(signalbkgbsi, coupling):
            for year in 2017, 2018:
              yield cls(year, signalbkgbsi, width, coupling, finalstate)
