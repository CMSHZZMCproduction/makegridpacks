#!/usr/bin/env python

import argparse, collections, glob, os, re, shutil, subprocess, sys

from utilities import cd, cdtemp, genproductions

def replacepowheg(oldfilename, newfilename, powhegprocess, jhugen):
  oldfilename = os.path.abspath(oldfilename)
  newfilename = os.path.abspath(newfilename)

  with cdtemp() as tmpfolder:
    subprocess.check_call(["scram", "p", "CMSSW", os.environ["CMSSW_VERSION"]])
    with cd(os.path.join(os.environ["CMSSW_VERSION"], "src")):
      for filename in glob.iglob(os.path.join(genproductions, "bin", "Powheg", "*")):
        if filename.endswith(".py") or filename.endswith(".sh") or filename.endswith("/patches") or filename.endswith("/examples"):
          os.symlink(filename, os.path.basename(filename))

      card = "examples/gg_H_quark-mass-effects_withJHUGen_NNPDF30_13TeV/gg_H_quark-mass-effects_NNPDF30_13TeV.input"
      JHUGencard = "examples/gg_H_quark-mass-effects_withJHUGen_NNPDF30_13TeV/JHUGen.input"
      command = ["./run_pwg.py", "-i", card, "-m", powhegprocess, '-f', "tmp", "-p", "0", "-d", "1"]
      if jhugen: command += ["-g", JHUGencard]
      subprocess.check_call(command)

      with cd("tmp"):
        newcompiledfolder = os.getcwd()

    with cdtemp():
      subprocess.check_call(["tar", "xvaf", oldfilename])

      if not os.path.exists("original_pwhg_main"):
        shutil.move("pwhg_main", "original_pwhg_main")
      shutil.move(os.path.join(newcompiledfolder, "pwhg_main"), "pwhg_main")
      if jhugen:
        if not os.path.exists("original_JHUGen"):
          shutil.move("JHUGen", "original_JHUGen")
        shutil.move(os.path.join(newcompiledfolder, "JHUGen"), "pwhg_main")


      subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  p.add_argument("--jhugen", action="store_true")
  p.add_argument("--powheg-process", required=True)
  args = p.parse_args()
  prunepwgrwl(**args.__dict__)
