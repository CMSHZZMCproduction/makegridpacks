from patchmcfmgridpack import patchmcfmgridpack

def dopatch(functionname, oldfilename, newfilename, **kwargs):
  return {
    "patchmcfmgridpack": patchmcfmgridpack,
  }[functionname](oldfilename=oldfilename, newfilename=newfilename, **kwargs)
