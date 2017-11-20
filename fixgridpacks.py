#!/usr/bin/env python

from makegridpacks import *

class MCSampleFix(MCSample):
  def fixgridpack(self):
    if self.tarballversion != 2: return "this tarball is not v2"
    mkdir_p(os.path.dirname(self.foreostarball))
    with KeepWhileOpenFile(self.foreostarball+".tmp", message=LSB_JOBID()) as kwof:
      if os.path.exists(self.cvmfstarball) or os.path.exists(self.eostarball) or os.path.exists(self.foreostarball): return "already fixed"
      if not kwof: return "another job is currently fixing this one"
      with cd(mkdtemp()):
        oldtarball = self.cvmfstarball
        if self == type(self)("WminusH", "4l", 400): oldtarball = self.eostarball
        oldtarball = oldtarball.replace("/v2/", "/v1/")
        subprocess.check_call(["tar", "xvzf", oldtarball.replace("/v2/", "/v1/")])
        os.remove("JHUGen.input")
        shutil.copy(self.JHUGencard, "JHUGen.input")
        subprocess.check_call(["tar", "cvzf", self.foreostarball]+os.listdir("."))
        shutil.rmtree(os.getcwd())
    return "fixed JHUGen decay card"

if __name__ == "__main__":
  for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
    for decaymode in "4l",:
      for mass in getmasses(productionmode, decaymode):
        sample = MCSampleFix(productionmode, decaymode, mass)
        print sample, sample.fixgridpack()
