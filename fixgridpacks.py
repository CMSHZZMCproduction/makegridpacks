#!/usr/bin/env python

from makegridpacks import *

@contextlib.contextmanager
def JHUGen():
  originaldir = os.getcwd()
  with cdtemp():
    subprocess.check_call(["wget", "http://spin.pha.jhu.edu/Generator/JHUGenerator.v7.0.11.tar.gz"])
    subprocess.check_call(["tar", "xvzf", "JHUGenerator.v7.0.11.tar.gz"])
    with cd("JHUGenerator"):
      subprocess.check_call(["sed", "-i", "-e", "s#linkMELA = Yes#linkMELA = No#g", "makefile"])
      os.system("make")
      output = subprocess.check_output(["./JHUGen", "Process=50", "VegasNc0=1000", "VegasNc2=1"])
      version = re.search("JHU Generator (v[0-9.]*)", output).group(1)
      if version != "v7.0.11": raise RuntimeError("Bad version "+version)
      filename = os.path.abspath("JHUGen")
      with cd(originaldir):
        yield filename

class MCSampleFix(MCSample):
  def fixgridpack(self, jhugen):
    mkdir_p(os.path.dirname(self.foreostarball))
    with KeepWhileOpenFile(self.foreostarball+".tmp", message=LSB_JOBID()) as kwof:
      if not kwof: return "another job is currently fixing this one"
      for _ in self.cvmfstarball, self.eostarball, self.foreostarball:
        if os.path.exists(_):
          with cdtemp():
            subprocess.check_output(["tar", "xvzf", _])
            output = subprocess.check_output(["./JHUGen", "Process=50", "VegasNc0=1000", "VegasNc2=1"])
            version = re.search("JHU Generator (v[0-9.]*)", output).group(1)
            if version == "v7.0.11": return "already fixed"
            if version == "v7.0.9": return "increment the version number"
            return "Unknown version "+version

      with cdtemp():
        oldtarball = self.cvmfstarball
        oldtarball = re.sub("(/v)([0-9]*)(/)", lambda match: match.group(1) + str(int(match.group(2))-1) + match.group(3), oldtarball)
        subprocess.check_output(["tar", "xvzf", oldtarball])
        os.remove("JHUGen")
        shutil.copy(jhugen, "JHUGen")
        subprocess.check_call(["tar", "cvzf", self.foreostarball]+os.listdir("."))
    return "fixed JHUGen version"

if __name__ == "__main__":
  with JHUGen() as jhugen:
    for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
      for decaymode in "4l", "2l2q", "2l2nu":
        for mass in getmasses(productionmode, decaymode):
          sample = MCSampleFix(productionmode, decaymode, mass)
          print sample, sample.fixgridpack(jhugen)
