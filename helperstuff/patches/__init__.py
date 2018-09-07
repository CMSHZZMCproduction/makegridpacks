from addJHUGentomadgraph import addJHUGentomadgraph
from patchmcfmgridpack import patchmcfmgridpack
from prunepwgrwl import prunepwgrwl

functiondict = {
  function.__name__: function
    for function in (addJHUGentomadgraph, patchmcfmgridpack, prunepwgrwl)
}

def dopatch(functionname, oldfilename, newfilename, **kwargs):
  return functiondict[functionname](oldfilename=oldfilename, newfilename=newfilename, **kwargs)
