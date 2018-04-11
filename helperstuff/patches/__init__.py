from patchmcfmgridpack import patchmcfmgridpack

functiondict = {
  "patchmcfmgridpack": patchmcfmgridpack,
}

def dopatch(functionname, oldfilename, newfilename, **kwargs):
  return functiondict[functionname](oldfilename=oldfilename, newfilename=newfilename, **kwargs)
