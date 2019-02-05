#!/usr/bin/env python

import os

from utilities import here

def cleanupgridpacks(folder):
  removed = []
  for dirpath, dirnames, filenames in os.walk(folder, topdown=False):
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
  cleanupgridpacks(os.path.join(here, "gridpacks"))
  cleanupgridpacks(os.path.join(here, "test_gridpacks"))
  cleanupgridpacks(os.path.join(here, "workdir"))
