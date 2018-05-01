#!/usr/bin/env python

from helperstuff import allsamples
import os

def tarballexists(s):
  try: return os.path.exists(s.cvmfstarball)
  except AssertionError: return True

for s in allsamples(onlymysamples = False, filter=lambda x: "MCFM" in str(type(x))):
  if not s.finished and tarballexists(s) and s.prepid and s.status in ("new", "validation", "defined"):
    try:
      if s.fullfragment != s.fullinfo["fragment"]: s.needsupdate = True
    except ValueError as e:
      if "mdatacard != mdatagitcard" not in str(e): raise
      s.needsupdate = True
  else:
    print s
