#!/usr/bin/env python

from makegridpacks import *

import argparse

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("--raise-error", action="store_true")
  p.add_argument("--start-from", help="skip all samples before this one")
  args = p.parse_args()
  seen = False
  if args.start_from is None:
    seen = True
  for sample in allsamples(lambda x: not x.finished and hasattr(x, "cvmfstarball") and os.path.exists(x.cvmfstarball)):
    if str(sample) == args.start_from: seen = True
    if not seen: continue
    if args.raise_error: print sample
    try:
      sample.getcardsurl()
    except Exception as e:
      if args.raise_error: raise
      etext = str(e).replace(str(sample), "").strip()
      print sample, etext
