from addJHUGentomadgraph import addJHUGentomadgraph
from patchmcfmgridpack import patchmcfmgridpack
from prunepwgrwl import prunepwgrwl
from tweakseed import tweakseed

functiondict = {
  function.__name__: function
    for function in (addJHUGentomadgraph, patchmcfmgridpack, prunepwgrwl, tweakseed)
}

def dopatch(functionname, oldfilename, newfilename, **kwargs):
  return functiondict[functionname](oldfilename=oldfilename, newfilename=newfilename, **kwargs)
