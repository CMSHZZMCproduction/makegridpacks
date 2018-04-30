#!/usr/bin/env python

import os

from utilities import here

def cleanupgridpacks():
  removed = []
  for dirpath, dirnames, filenames in os.walk(os.path.join(here, "gridpacks"), topdown=False):
    for _ in dirnames[:]:
      if os.path.join(dirpath, _) in removed:
        dirnames.remove(_)
    if not dirnames and not filenames:
      os.rmdir(dirpath)
      removed.append(dirpath)

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()
  args = parser.parse_args()
  cleanupgridpacks()
