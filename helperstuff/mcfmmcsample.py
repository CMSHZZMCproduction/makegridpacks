
from utilities import cache, cd, cdtemp, cmsswversion, genproductions, here, makecards, mkdir_p, scramarch, wget

from mcsamplebase import MCSampleBase

import abc, os, contextlib, urllib, re, filecmp, glob, pycurl, shutil, stat, subprocess, itertools

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
	'-d': self.datasetname,
	'-q': self.queue
	}
    return ['./run_mcfm_AC.py'] + sum(([k] if v is None else [k, v] for k, v in args.iteritems()), [])
 
  @property
  def makinggridpacksubmitsjob(self):
    return 'MCFM_submit_%s.sh'%(self.datasetname)

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
      for root, dirs, files in os.walk("."):
	for ifile in files:
	  try:
	    os.stat(ifile)
	  except Exception as e: 
	    if e.args == 'No such file or directory':   continue
	    print ifile
	    print e.message, e.args
    	    raise ValueError("There is a broken symlink in the tarball\n{}".format(self))
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

    if differentproductioncards(productioncard,productiongitcard):
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

    if mdatacard != mdatagitcard:
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
