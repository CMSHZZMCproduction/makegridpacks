import glob, subprocess

from utilities import cdtemp

def prunepwgrwl(oldfilename, newfilename, sample):
  with cdtemp():
    subprocess.check_call(["tar", "xvaf", oldfilename])
    sample.editpwgrwl()
    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))
