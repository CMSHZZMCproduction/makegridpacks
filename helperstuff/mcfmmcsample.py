import abc, os, contextlib, re, filecmp, glob, pycurl, shutil, stat, subprocess, itertools, os

import uncertainties

from jobsubmission import jobid, jobtype
from utilities import cache, cd, cdtemp, genproductions, here, jobexitstatusfromlog, jobended, KeepWhileOpenFile, makecards, mkdir_p, osversion, wget, urlopen

from jhugenmcsample import UsesJHUGenLibraries
from mcsamplebase import MCSampleBase
from mcsamplewithxsec import MCSampleWithXsec, NoXsecError

def differentproductioncards(productioncard, gitproductioncard):
	allowedtobediff = ['[readin]','[writeout]','[ingridfile]','[outgridfile]']
	prodcardintarball = [line for line in productioncard.split('\n') if line != '']
	prodcardongit = [line for line in gitproductioncard.split('\n') if line != '']
	if len(prodcardintarball) != len(prodcardongit):
		print self.cvmfstarball
		print 'len(prodcardintarball) != len(prodcardongit)'
		return True
	diffprodcard = False
	for tline, gline in itertools.izip(prodcardintarball,prodcardongit):
		if '[LHAPDF group]' in tline:
			if tline.split()[0] != gline.split()[0]:
				print str(tline.split()[0])+ '!=' +str(gline.split()[0])
				return True
			else:	continue
		if any(item in tline for item in allowedtobediff):
			continue
		elif tline == gline:
			continue
		else:
			print tline
			print gline
			return True
	return diffprodcard
			
			

class MCFMMCSample(UsesJHUGenLibraries, MCSampleWithXsec):

  def checkandfixtarball(self):
    mkdir_p(self.workdirforgridpack)
    with KeepWhileOpenFile(os.path.join(self.workdirforgridpack,self.prepid+'.tmp'),deleteifjobdied=True) as kwof:
	if not kwof: return " check in progress"
	if not jobtype(): self.submitLSF(); return "Check if the tarball needs fixing"	
  	with cdtemp():
  	  subprocess.call(['cp',self.cvmfstarball,'.'])
  	  subprocess.check_call(['tar','xzvf',self.cvmfstarball])
  	  subprocess.call(['cp','readInput.DAT','readInput.DAT_bak'])
	  os.system('chmod 755 runcmsgrid.sh')
	  try:
  	    output = subprocess.check_output(['bash','runcmsgrid.sh','1','31313','12'], stderr=subprocess.STDOUT)
	  except subprocess.CalledProcessError as e:
	    output = e.output
  	  for line in output.split('\n'):
  	    if not 'Reading in vegas grid from' in line: continue
  	    else:
  	      line = line.split()[-2]
  	      internalgridname = line.split('CMS_')[1]
	  internalgridname = str(internalgridname)
	  print "internal tarball name: "+internalgridname
  	  if self.datasetname+'_grid' == internalgridname:
	    with open(os.path.join(self.workdirforgridpack,'INTACT'),'w') as fout:
	      fout.write(jobid())
  	    return str(self.identifiers)+"'s gridpack is intact"
  	  else:
  	    os.system('cp '+self.datasetname+'_grid '+internalgridname)
 	    os.system('mv readInput.DAT_bak readInput.DAT')
  	    os.system('rm -r *tgz CMSSW*')
  	    curdirpath = subprocess.check_output(['pwd'])
  	    os.system('tar cvaf '+self.tmptarball+' ./*')
	    if os.path.exists(self.tmptarball):	
	      with open(os.path.join(self.workdirforgridpack,'FIXED'),'w') as fout:
		fout.write(jobid())


  @property 
  def method(self):	return 'mdata'
  @abc.abstractproperty
  def productioncard(self): pass
  @property
  def cardbase(self):
    return os.path.basename(self.productioncard).split(".DAT")[0]
  @property
  def tmptarballbasename(self):
    return "MCFM_%s_%s_%s_%s.tgz" % (self.method, self.scramarch, self.cmsswversion, self.datasetname)
  @property
  def makegridpackcommand(self):
    args = {
	'-i': self.productioncard,
	'--coupling': self.coupling,
	'--bsisigbkg': self.signalbkgbsi,
	'-d': self.datasetname,
	'-q': self.creategridpackqueue,
	'-s': str(hash(self) % 2147483647 + self.tweakmakegridpackseed),
	}
    if osversion == 6:
      args["--slc6"] = None
    return ['./run_mcfm_AC.py'] + sum(([k] if v is None else [k, v] for k, v in args.iteritems()), [])

  @property
  def makinggridpacksubmitsjob(self):
    return True
  @property
  def gridpackjobsrunning(self):
    if not os.path.exists(self.workdirforgridpack):
      return False
    with cd(self.workdirforgridpack):
      logs = glob.glob("condor.*.log")
      if len(logs) > 1: raise RuntimeError("Multiple logs in "+self.workdirforgridpack)
      if not logs: return False
      exitstatus = jobexitstatusfromlog(logs[0], okiffailed=True)
      if exitstatus is None: return True
      return False
 
  def getxsec(self):
    with cdtemp():
      if not os.path.exists(self.cvmfstarball): raise NoXsecError
      subprocess.check_output(["tar", "xvaf", self.cvmfstarball])
      dats = set(glob.iglob("*.dat")) - {"fferr.dat", "ffperm5.dat", "ffwarn.dat", "hto_output.dat"}
      if len(dats) != 1:
        raise ValueError("Expected to find exactly 1 .dat in the tarball\n"
                         "(besides fferr.dat, ffperm5.dat, ffwarn.dat, hto_output.dat)\n"
                         "but found {}:\n{}\n\n{}".format(len(dats), ", ".join(dats), self.cvmfstarball))
      with open(dats.pop()) as f:
        matches = re.findall(r"Cross-section is:\s*([0-9.Ee+-]*)\s*[+]/-\s*([0-9.Ee+-]*)\s*", f.read())
      if not matches: raise ValueError("Didn't find the cross section in the dat\n\n"+self.cvmfstarball)
      if len(matches) > 1: raise ValueError("Found multiple cross section lines in the dat\n\n"+self.cvmfstarball)
      xsec, xsecerror = matches[0]
      return uncertainties.ufloat(xsec, xsecerror)

  @property
  def cardsurl(self):
    commit = self.genproductionscommit
    productioncardurl = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.productioncard.split("genproductions/")[-1])
    mdatascript = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, "bin/MCFM/ACmdataConfig.py")
    with cdtemp():
      with contextlib.closing(urlopen(productioncardurl)) as f:
        productiongitcard = f.read()

#    for root, dirs, files in os.walk("."):
#      for ifile in files:
#        try:
#          os.stat(ifile)
#        except Exception as e: 
#          if e.args == 'No such file or directory':   continue
#          print ifile
#          print e.message, e.args
#          raise ValueError("There is a broken symlink in the tarball\n{}".format(self))
    try:
      with open("readInput.DAT") as f:
        productioncard = f.read()
    except IOError:
      raise ValueError("no readInput.DAT in the tarball\n{}".format(self)) 
    try:
      with open("src/User/mdata.f") as f:
        mdatacard = f.read()
    except IOError:
      raise ValueError("no src/User/mdata.f in the tarball\n{}".format(self))

    if differentproductioncards(productioncard,productiongitcard) and not 'BKG' in self.identifiers:
      with cd(here):
        with open("productioncard", "w") as f:
          f.write(productioncard)
        with open("productiongitcard", "w") as f:
          f.write(productiongitcard)
      raise ValueError("productioncard != productiongitcard\n{}\nSee ./productioncard and ./productiongitcard".format(self))

    with contextlib.closing(urlopen(os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/"+commit+"/bin/MCFM/run_mcfm_AC.py"))) as f:
      infunction = False
      for line in f:
        if re.match(r"^\s*def .*", line): infunction = False
        if re.match(r"^\s*def downloadmcfm.*", line): infunction = True
        if not infunction: continue
        match = re.search(r"git checkout ([\w.]*)", line)
        if match: mcfmcommit = match.group(1)
    with cdtemp():
      wget("http://spin.pha.jhu.edu/Generator/JHUGenerator."+self.JHUGenversion+".tar.gz")
      subprocess.check_output(["tar", "xvzf", "JHUGenerator."+self.JHUGenversion+".tar.gz"])
      mkdir_p("src/User")
      shutil.move("MCFM-JHUGen/src/User/mdata.f", "src/User/")
      wget(mdatascript)
      subprocess.check_call(["python", os.path.basename(mdatascript), "--coupling", self.coupling, "--mcfmdir", ".", "--bsisigbkg", self.signalbkgbsi])
      with open("src/User/mdata.f") as f:
        mdatagitcard = f.read()

    if mdatacard != mdatagitcard and not 'BKG' in self.identifiers:
      with cd(here):
        with open("mdatacard", "w") as f:
          f.write(mdatacard)
        with open("mdatagitcard", "w") as f:
          f.write(mdatagitcard)
      raise ValueError("mdatacard != mdatagitcard\n{}\nSee ./mdatacard and ./mdatagitcard".format(self))

    result = (       productioncardurl + "\n"
            + "# " + mdatascript + "\n"
            + "#    --coupling " + self.coupling + " --bsisigbkg " + self.signalbkgbsi)

    moreresult = super(MCFMMCSample, self).cardsurl
    if moreresult: result += "\n# " + moreresult

    return result

  @property
  def productiongenerators(self):
    return ["MCFM701"] + super(MCFMMCSample, self).productiongenerators
  @property
  def JHUGenversion(self): return "v7.3.0"

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin","MCFM", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename

  @property
  def JHUGenlocationintarball(self):
    return None

  def findPDFfromtarball(self):
    name = None
    memberid = 0
    with open("readInput.DAT") as f:
      for line in f:
        match = re.match(r"'([^']+)'\s*\[LHAPDF group\]", line)
        if match: name = match.group(1)
        match = re.match(r"([0-9]+)\s*\[LHAPDF set\]", line)
        if match: memberid = int(match.group(1))
    if name is None:
      raise ValueError("No LHAPDF group in readInput.DAT")
    if memberid is None:
      raise ValueError("No LHAPDF set in readInput.DAT")
    return name, memberid

