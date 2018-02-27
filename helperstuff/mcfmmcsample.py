
from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, mkdir_p, scramarch, wget, KeepWhileOpenFile, LSB_JOBID, jobended

from mcsamplebase import MCSampleBase

import abc, os, contextlib, urllib, re, filecmp, glob, pycurl, shutil, stat, subprocess, itertools, os

def differentproductioncards(productioncard, gitproductioncard):
	allowedtobediff = ['[readin]','[writeout]','[ingridfile]','[outgridfile]']
	prodcardintarball = [line for line in productioncard.split('\n') if line != '']
	prodcardongit = [line for line in gitproductioncard.split('\n') if line != '']
	if len(prodcardintarball) != len(prodcardongit):
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
			
			

class MCFMMCSample(MCSampleBase):

  def checkandfixtarball(self):
    mkdir_p(self.workdir)
    with KeepWhileOpenFile(os.path.join(self.workdir,self.prepid+'.tmp'),message=LSB_JOBID(),deleteifjobdied=True) as kwof:
	if not kwof: return " check in progress"
	if not LSB_JOBID(): self.submitLSF(); return "Check if the tarball needs fixing"	
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
	    with open(os.path.join(self.workdir,'INTACT'),'w') as fout:
	      fout.write(LSB_JOBID())
  	    return str(self.identifiers)+"'s gridpack is intact"
  	  else:
  	    os.system('cp '+self.datasetname+'_grid '+internalgridname)
 	    os.system('mv readInput.DAT_bak readInput.DAT')
  	    os.system('rm -r *tgz CMSSW*')
  	    curdirpath = subprocess.check_output(['pwd'])
  	    os.system('tar cvzf '+self.tmptarball+' ./*')
	    if os.path.exists(self.tmptarball):	
	      with open(os.path.join(self.workdir,'FIXED'),'w') as fout:
		fout.write(LSB_JOBID())


  @property 
  def method(self):	return 'mdata'
  @abc.abstractproperty
  def productioncard(self): pass
  @property
  def cardbase(self):
    return os.path.basename(self.productioncard).split(".DAT")[0]
  @property
  def tmptarball(self):
    return os.path.join(here, "workdir", self.datasetname, "MCFM_%s_%s_%s_%s.tgz" % (self.method, scramarch, cmsswversion, self.datasetname))
  @property
  def makegridpackcommand(self):
    args = {
	'-i': self.productioncard,
	'--coupling': self.coupling,
	'--bsisigbkg': self.signalbkgbsi,
	'-d': self.datasetname,
	'-q': self.creategridpackqueue
	}
    return ['./run_mcfm_AC.py'] + sum(([k] if v is None else [k, v] for k, v in args.iteritems()), [])
 
  @property
  def makinggridpacksubmitsjob(self):
    return 'MCFM_submit_%s.sh'%(self.datasetname)

  def getxsec(self, error=False):
    with cdtemp():
      subprocess.check_output(["tar", "xvzf", self.cvmfstarball])
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
      self.xsec = float(xsec)
      self.xsecerror = float(xsecerror)
      return self.xsecerror if error else self.xsec

  @property
  def notes(self):
    return "cross section = {} +/- {}".format(self.xsec, self.xsecerror)

  @property
  def xsec(self):
    with cd(here):
      try:
        return self.value["xsec"]
      except KeyError:
        self.getxsec()
        return self.xsec
  @xsec.setter
  def xsec(self, value):
    with cd(here), self.writingdict():
      self.value["xsec"] = value
  @xsec.deleter
  def xsec(self):
    with cd(here), self.writingdict():
      del self.value["xsec"]

  @property
  def xsecerror(self):
    with cd(here):
      try:
        return self.value["xsecerror"]
      except KeyError:
        self.getxsec()
        return self.xsecerror
  @xsecerror.setter
  def xsecerror(self, value):
    with cd(here), self.writingdict():
      self.value["xsecerror"] = value
  @xsecerror.deleter
  def xsecerror(self):
    with cd(here), self.writingdict():
      del self.value["xsecerror"]

  @property
  @cache
  def cardsurl(self):
    commit = self.genproductionscommit
    print commit
    productioncardurl = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", commit, self.productioncard.split("genproductions/")[-1])
    mdatascript = os.path.join("https://raw.githubusercontent.com/carolhungwt/genproductions/", commit, "bin/MCFM/ACmdataConfig.py")
    with cdtemp():
      with contextlib.closing(urllib.urlopen(productioncardurl)) as f:
        productiongitcard = f.read()

    with cdtemp():
      subprocess.check_output(["tar", "xvzf", self.cvmfstarball])
      if glob.glob("core.*"):
        raise ValueError("There is a core dump in the tarball\n{}".format(self))
#      for root, dirs, files in os.walk("."):
#	for ifile in files:
#	  try:
#	    os.stat(ifile)
#	  except Exception as e: 
#	    if e.args == 'No such file or directory':   continue
#	    print ifile
#	    print e.message, e.args
 #   	    raise ValueError("There is a broken symlink in the tarball\n{}".format(self))
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

    with contextlib.closing(urllib.urlopen(os.path.join("https://raw.githubusercontent.com/carolhungwt/genproductions/8b7ab33b0805c965c53ee2f1f3980abcde13f41a/bin/MCFM/run_mcfm_AC.py"))) as f:
      infunction = False
      for line in f:
        if re.match(r"^\s*def .*", line): infunction = False
        if re.match(r"^\s*def downloadmcfm.*", line): infunction = True
        if not infunction: continue
        match = re.search(r"git checkout (\S*)", line)
        if match: mcfmcommit = match.group(1)
    with cdtemp():
      mkdir_p("src/User")
      with cd("src/User"): wget(os.path.join("https://raw.githubusercontent.com/usarica/MCFM-7.0_JHUGen", mcfmcommit, "src/User/mdata.f"))
      wget(mdatascript)
      #subprocess.check_call(["python", os.path.basename(mdatascript), "--coupling", self.coupling, "--mcfmdir", "."])
      with open("src/User/mdata.f") as f:
        mdatagitcard = f.read()

    if mdatacard != mdatagitcard and not 'BKG' in self.identifiers:
      with cd(here):
        with open("mdatacard", "w") as f:
          f.write(mdatacard)
        with open("mdatagitcard", "w") as f:
          f.write(mdatagitcard)
     # raise ValueError("mdatacard != mdatagitcard\n{}\nSee ./mdatacard and ./mdatagitcard".format(self))

    result = (       productioncardurl + "\n"
            + "# " + mdatascript + "\n"
            + "#    " + self.coupling)

    return result

  @property
  def generators(self):
    return ["MCFM701", "JHUGen v7.0.11"]

  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "..","scriptsforMCFM", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename
