import os, shutil

from itertools import izip

from utilities import cd, cdtemp

from addJHUGentomadgraph import addJHUGentomadgraph
from parallelizepowheg import parallelizepowheg
from patchmcfmgridpack import patchmcfmgridpack
from prunepwgrwl import prunepwgrwl
from replacepowheg import replacepowheg
from tweakseed import tweakseed

def multiplepatches(oldfilename, newfilename, listofkwargs):
  cdminus = os.getcwd()
  results = []
  if len(listofkwargs) == 0:
    shutil.copy(oldfilename, newfilename)
    return results
  with cdtemp() as tmpdir, cd(cdminus):
    base, extension = os.path.split(oldfilename)
    oldfilenames = [oldfilename] + [os.path.join(tmpdir, "tmp{}.{}".format(i, extension)) for i in range(1, len(listofkwargs))]
    newfilenames = oldfilenames[1:] + [newfilename]
    for kwargs, oldfilename, newfilename in izip(listofkwargs, oldfilenames, newfilenames):
      kwargs = kwargs.copy()
      if "oldfilename" in kwargs or "newfilename" in kwargs: raise TypeError("can't provide oldfilename or newfilename in the individual kwargs for multiplepatches\n\n{}".format(kwargs))
      kwargs.update(oldfilename=oldfilename, newfilename=newfilename)
      results.append(dopatch(**kwargs))

  return results

functiondict = {
  function.__name__: function
    for function in (addJHUGentomadgraph, parallelizepowheg, patchmcfmgridpack, prunepwgrwl, tweakseed, multiplepatches, replacepowheg)
}

def dopatch(functionname, oldfilename, newfilename, **kwargs):
  return functiondict[functionname](oldfilename=oldfilename, newfilename=newfilename, **kwargs)
