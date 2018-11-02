#!/usr/bin/env python
import argparse
import glob
import os
import re
import shutil
import stat
import subprocess

from utilities import cdtemp, genproductions

def patchmcfmgridpack(oldfilename, newfilename):
  oldfilename = os.path.abspath(oldfilename)
  newfilename = os.path.abspath(newfilename)

  with cdtemp():
    subprocess.check_call(["tar", "xvaf", oldfilename])
    with open("runcmsgrid.sh") as f:
      contents = f.read()

      SCRAM_ARCH_VERSION_REPLACE = set(re.findall(r"scram_arch_version=([^$\s]+)", contents))
      assert len(SCRAM_ARCH_VERSION_REPLACE) == 1, SCRAM_ARCH_VERSION_REPLACE
      SCRAM_ARCH_VERSION_REPLACE = SCRAM_ARCH_VERSION_REPLACE.pop()

      CMSSW_VERSION_REPLACE = set(re.findall(r"cmssw_version=([^$\s]+)", contents))
      assert len(CMSSW_VERSION_REPLACE) == 1, CMSSW_VERSION_REPLACE
      CMSSW_VERSION_REPLACE = CMSSW_VERSION_REPLACE.pop()

    shutil.copy(os.path.join(genproductions, "bin", "MCFM", "runcmsgrid_template.sh"), "runcmsgrid.sh")
    shutil.copy(os.path.join(genproductions, "bin", "MCFM", "adjlheevent.py"), ".")

    with open("runcmsgrid.sh") as f:
      contents = (f.read()
                   .replace("SCRAM_ARCH_VERSION_REPLACE", SCRAM_ARCH_VERSION_REPLACE)
                   .replace("CMSSW_VERSION_REPLACE", CMSSW_VERSION_REPLACE)
                   .replace("./mcfm INPUT.DAT", "./Bin/mcfm readInput.DAT")
                   .replace("INPUT.DAT", "readInput.DAT")
      )
    with open("runcmsgrid.sh", "w") as f:
      f.write(contents)

    os.chmod("runcmsgrid.sh", os.stat("runcmsgrid.sh").st_mode | stat.S_IEXEC)

    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  args = p.parse_args()
  patchmcfmgridpack(**args.__dict__)
