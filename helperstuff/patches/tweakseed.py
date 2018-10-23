#!/usr/bin/env python

import argparse, collections, glob, os, re, shutil, subprocess, sys

from ..utilities import cache, cdtemp, OrderedCounter

sys.path.append(os.path.join(os.environ["LHAPDF_DATA_PATH"], "..", "..", "lib", "python2.7", "site-packages"))

def tweakseed(oldfilename, newfilename, increaseby, verbose=False):
  oldfilename = os.path.abspath(oldfilename)
  newfilename = os.path.abspath(newfilename)

  with cdtemp():
    subprocess.check_call(["tar", "xvaf", oldfilename])

    if not os.path.exists("original-runcmsgrid.sh"):
      shutil.move("runcmsgrid.sh", "original-runcmsgrid.sh")

    with open("original-runcmsgrid.sh") as f, open("runcmsgrid.sh", "w") as newf:
      contents = f.read()
      if contents.count("${2}") != 1:
        raise ValueError("{}\n\n\n${{2}} appears {} times in ^^^ runcmsgrid.sh".format(contents, contents.count("${2}")))
      contents = contents.replace("${2}", "$(expr ${{2}} + {})".format(increaseby))
      newf.write(contents)

    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  p.add_argument("--increaseby", required=True, type=int)
  p.add_argument("-v", "--verbose", action="store_true")
  args = p.parse_args()
  tweakseed(**args.__dict__)
