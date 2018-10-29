#!/usr/bin/env python

from makegridpacks import *

import argparse

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("--raise-error", action="store_true")
  args = p.parse_args()
  for sample in allsamples(lambda x: hasattr(x, "cvmfstarball") and os.path.exists(x.cvmfstarball)):
    try:
      sample.getcardsurl()
    except Exception as e:
      if args.raise_error: raise
      etext = str(e).replace(str(sample), "").strip()
      print sample, etext
