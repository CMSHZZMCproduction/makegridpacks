#!/usr/bin/env python

import argparse, collections, glob, os, re, shutil, subprocess, sys

from utilities import cache, cd, cdtemp, OrderedCounter

JHUGenpart = """
echo "Doing JHUGen decay"
mv cmsgrid_final.lhe undecayed.lhe
./JHUGen $(cat InputCards/JHUGen.input) ReadLHE=undecayed.lhe DataFile=cmsgrid_final.lhe Seed=${rnum}
"""

def addJHUGentomadgraph(oldfilename, newfilename, JHUGenversion, decaycard):
  oldfilename = os.path.abspath(oldfilename)
  newfilename = os.path.abspath(newfilename)

  with cdtemp() as tmpdir:
    subprocess.check_call(["tar", "xvaf", oldfilename])

    if not os.path.exists("original_runcmsgrid.sh"):
      shutil.move("runcmsgrid.sh", "original_runcmsgrid.sh")

    with open("original_runcmsgrid.sh") as f, open("runcmsgrid.sh", "w") as newf:
      sawexit = False
      for line in f:
        if "exit" in line:
          if sawexit:
            raise IOError("Multiple exit lines in runcmsgrid.sh")
          newf.write(JHUGenpart)
          sawexit = True
        if "JHUGen" in line:
          raise IOError("runcmsgrid.sh already has JHUGen decay")
        newf.write(line)
      if not sawexit:
        newf.write(JHUGenpart)
    os.chmod("runcmsgrid.sh", os.stat("original_runcmsgrid.sh").st_mode)

    with cdtemp():
      subprocess.check_call(["wget", "http://spin.pha.jhu.edu/Generator/JHUGenerator."+JHUGenversion+".tar.gz"])
      subprocess.check_call(["tar", "xvzf", "JHUGenerator."+JHUGenversion+".tar.gz"])
      with cd("JHUGenerator"):
        with open("makefile") as f:
          oldmakefile = f.read()
        newmakefile = re.sub("(linkMELA *= *)Yes", r"\1No", oldmakefile)
        assert re.search("linkMELA *= *No", newmakefile)
        with open("makefile", "w") as f:
          f.write(newmakefile)
        os.system("make")
        shutil.copy("JHUGen", tmpdir)

    shutil.copy(decaycard, "InputCards/JHUGen.input")

    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  p.add_argument("--jhugen-version", default="v7.1.4")
  p.add_argument("--jhugen-card", required=True)
  args = p.parse_args()
  addJHUGentomadgraph(**args.__dict__)
