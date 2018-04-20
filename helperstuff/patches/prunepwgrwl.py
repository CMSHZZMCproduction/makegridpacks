import glob, subprocess

from utilities import cdtemp

def prunepwgrwl(oldfilename, newfilename, sample):
  with cdtemp():
    subprocess.check_call(["tar", "xvzf", oldfilename])
    sample.editpwgrwl()
    subprocess.check_call(["tar", "cvzf", newfilename] + glob.glob("*"))
