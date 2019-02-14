"""
copy this to a new file, and rename the class
search for "fill this" to see all the things that need to be filled in
"""

import abc, contextlib, glob, os, re, subprocess

from utilities import cache, cacheaslist, cd, cdtemp, genproductions, here, makecards, urlopen, wget

from mcsamplebase import MCSampleBase

class MyMCSample(MCSampleBase):
  def __init__(self):
    """
    fill this, and add more arguments to the function
    typically all the identifiers in identifiers below should be set here
    """
  @property
  def identifiers(self):
    """
    fill this, very important!
    the identifiers here should uniquely identify this sample
    they should be set in __init__
    """
  @property
  def nevents(self):
    """
    fill this, number of events to be produced
    """


  @property
  def hasfilter(self):
     "fill this (typically false, if there's a JHUGen or Pythia filter then true)"
  @property
  def tmptarballbasename(self):
    """
    fill this
    this has to be whatever is created by the script that makes the tarball
    """
  @property
  def tarballversion(self):
    v = 1
    """
    if the first tarball is copied to eos and then is found to be bad, add something like
    if self.(whatever) == (whatever): v += 1
    """
    return v

  def cvmfstarball_anyversion(self, version):
    """
    fill this
    folder = "/cvmfs/cms.cern.ch/phys_generator/gridpacks/..."
    tarballname = "..."
    return os.path.join(folder, tarballname.replace(".tgz", ""), "v{}".format(version), tarballname)
    """

  @property
  def datasetname(self):
    """
    fill this
    """

  @property
  def defaulttimeperevent(self):
    """
    fill this, this is how long roughly you expect each event to take
    it doesn't have to be too precise
    """
  @property
  def tags(self):
    """
    fill this
    for example ["HZZ", "Fall17P2A"]
    """

  @property
  def genproductionscommit(self):
    """
    fill this
    just make sure it's AFTER any relevant pull requests
    """

  @classmethod
  @cacheaslist
  def allsamples(cls):
    """
    fill this
    example (from powheg, would need another classmethod named getmasses)
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2q", "2l2nu":
        for mass in cls.getmasses(productionmode, decaymode):
          yield cls(productionmode, decaymode, mass)
    """

  @property
  def responsible(self):
    "fill this (returns an lxplus username)"

  @property
  def makegridpackcommand(self):
    """
    fill this
    it's supposed to be a list of command line arguments
    for example ["./run_pwg.py", "-p", ...]
    """
  @property
  def makinggridpacksubmitsjob(self):
    """
    fill this
    if the script to create a gridpack (e.g. run_pwg.py) submits a job, return the job name
    otherwise return False or None
    """
  @property
  def cardsurl(self):
    """
    fill this
    in the simplest case, it's just os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", self.genproductionscommit, ...)
    if there are multiple cards (e.g. JHUGen decay), you have to put a "\n# " before the second one
    you may also want to do sanity checking here, for example

    card = os.path.join("https://raw.githubusercontent.com/cms-sw/genproductions/", self.genproductionscommit, ...)
    with cdtemp():
      wget(card)
      with open(os.path.basename(card)) as f:
        gitcardcontents = f.read()
    try:
      with open(<card name>) as f:
        cardcontents = f.read()
    except IOError:
      raise ValueError("no <card name> in the tarball\n{}".format(self))

    if cardcontents != gitcardcontents:
      with cd(here):
        with open("cardcontents", "w") as f:
          f.write(cardcontents)
        with open("powheggitcard", "w") as f:
          f.write(gitcardcontents)
      raise ValueError("cardcontents != gitcardcontents\n{}\nSee ./cardcontents and ./gitcardcontents".format(self))

    return card
    """

  @property
  def productiongenerators(self):
    "fill this, it's a list of generators, they get reported on the McM request"

  @property
  def fragmentname(self):
    """
    fill this (important!)
    it's Configuration/GenProduction/python/..., where ... comes from python in the genproductions repository
    """

  @property
  def makegridpackscriptstolink(self):
    """
    fill this
    it's basically the script to make the gridpack (e.g. run_pwg.py)
    and anything needed by it (e.g. other scripts, patches folder)
    """

  @property
  def creategridpackqueue(self):
    """fill this or delete it (default tomorrow)"""
  @property
  def timepereventqueue(self):
    """fill this or delete it (default tomorrow)"""
  @property
  def filterefficiencyqueue(self):
    """fill this or delete it (default tomorrow)"""

"""
finally, import this module in __init__.py in the helperstuff folder, inside the allsamples function
adding the words "fill this" here so that you can search for it
"""
