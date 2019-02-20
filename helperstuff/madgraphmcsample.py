import abc, contextlib, glob, itertools, os, re, subprocess

from utilities import cache, cd, cdtemp, genproductions, here, makecards, mkdir_p, wget

from mcsamplebase import MCSampleBase, MCSampleBase_DefaultCampaign

class MadGraphMCSample(MCSampleBase):
  @property
  def makegridpackscriptstolink(self):
    for filename in glob.iglob(os.path.join(genproductions, "bin", "MadGraph5_aMCatNLO", "*")):
      if (filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches")) and not os.path.exists(os.path.basename(filename)):
        yield filename
  def createtarball(self):
    return "making a madgraph tarball is not automated, you have to make it yourself and put it in {}".format(self.foreostarball)
  @property
  def tmptarballbasename(self):
    return "dummy.tgz"

  @property
  def cardsurl(self):
    def getcontents(f):
      contents = ""
      for line in f:
        line = line.split("!")[0]
        line = line.split("#")[0]
        line = line.strip()
        line = re.sub(" *= *", " = ", line)
        if not line: continue
        if line.startswith("define p = "): continue
        if line.startswith("define j = "): continue
        contents += line+"\n"
      return contents

    gitcardcontents = []
    if self.madgraphcardscript is None:
      cardurls = tuple(
        os.path.join(
          "https://raw.githubusercontent.com/cms-sw/genproductions/",
          self.genproductionscommit,
          (_[0] if len(_) == 2 else _).replace(genproductions+"/", "")
        ) for _ in self.madgraphcards
      )
      with cdtemp():
        for cardurl in cardurls:
          wget(cardurl)
          with open(os.path.basename(cardurl)) as f:
            gitcardcontents.append(getcontents(f))
    else:
      scripturls = tuple(
        os.path.join(
          "https://raw.githubusercontent.com/cms-sw/genproductions/",
          self.genproductionscommit,
          _.replace(genproductions+"/", "")
        ) for _ in self.madgraphcardscript
      )
      with cdtemp():
        wget(scripturls[0])
        for _ in scripturls[1:]:
          relpath = os.path.relpath(os.path.dirname(_), os.path.dirname(scripturls[0]))
          assert ".." not in relpath, relpath
          mkdir_p(relpath)
          with cd(relpath):
            wget(_)
        subprocess.check_call(["chmod", "u+x", os.path.basename(scripturls[0])])
        try:
          subprocess.check_output(["./"+os.path.basename(scripturls[0])], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
          print e.output
          raise
        for _ in self.madgraphcards:
          if len(_) == 2: _ = _[0]
          with open(_) as f:
            gitcardcontents.append(getcontents(f))


    cardnamesintarball = tuple(
      os.path.join("InputCards", os.path.basename(_[1] if len(_) == 2 else _))
      for _ in self.madgraphcards
    )
    cardcontents = []
    for cardnameintarball in cardnamesintarball:
      try:
        with open(cardnameintarball) as f:
          cardcontents.append(getcontents(f))
      except IOError:
        raise ValueError("no "+cardnameintarball+" in the tarball\n{}".format(self))
    for _ in glob.iglob("InputCards/*"):
      if _ not in cardnamesintarball and not _.endswith(".tar.gz") and not _.endswith(".~1~") and _ not in self.otherthingsininputcards:
        raise ValueError("unknown thing "+_+" in InputCards\n{}".format(self))

    for name, cc, gcc in itertools.izip(cardnamesintarball, cardcontents, gitcardcontents):
      _, suffix = os.path.splitext(os.path.basename(name))
      if cc != gcc:
        with cd(here):
          with open("cardcontents"+suffix, "w") as f:
            f.write(cc)
          with open("gitcardcontents"+suffix, "w") as f:
            f.write(gcc)
        raise ValueError(name + " in tarball != " + name + " in git\n{}\nSee ./cardcontents{} and ./gitcardcontents{}".format(self, suffix, suffix))

    if self.madgraphcardscript:
      result = "\n#    ".join((scripturls[0],) + tuple(self.madgraphcards))
    else:
      result = "\n# ".join(cardurls)

    moreresult = super(MadGraphMCSample, self).cardsurl
    if moreresult: result += "\n# " + moreresult

    return result


  @property
  def madgraphcardscript(self): return None

  @abc.abstractproperty
  def madgraphcards(self): return []

  @property
  def otherthingsininputcards(self): return []

  @property
  def productiongenerators(self):
    return super(MadGraphMCSample, self).productiongenerators + ["madgraph"]

  @property
  def makegridpackcommand(self):
    """
    if you implement this, you also HAVE to change tmptarballbasename to be the correct name
    it should be whatever is created by the script
    """
    assert False
  @property
  def makinggridpacksubmitsjob(self):
    assert False

  def handle_request_fragment_check_patch(self, line):
    if line.strip() == "* [PATCH] MG5_aMC@NLO LO nthreads patch not made in EOS":
      return "Needs madgraph LO nthreads patch"
    return super(MadGraphMCSample, self).handle_request_fragment_check_patch(line)
