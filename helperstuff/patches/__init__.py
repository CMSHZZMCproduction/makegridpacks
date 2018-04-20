from patchmcfmgridpack import patchmcfmgridpack
from prunepwgrwl import prunepwgrwl

functiondict = {
  "patchmcfmgridpack": patchmcfmgridpack,
  "prunepwgrwl": prunepwgrwl,
}

def dopatch(functionname, oldfilename, newfilename, **kwargs):
  return functiondict[functionname](oldfilename=oldfilename, newfilename=newfilename, **kwargs)
