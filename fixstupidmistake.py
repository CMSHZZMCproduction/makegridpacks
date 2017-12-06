#!/usr/bin/env python

from makegridpacks import *

with RequestQueue() as q, MCSample.writingdict() as f:
  MCSample.getdict(usekwof=False)
  for p in "ggH", "ZH", "ttH":
    for d in "4l", "2l2q", "2l2nu":
      for m in getmasses(p, d):
        print MCSample(p, d, m)
        s = MCSample(p, d, m)
        if not s.needsupdate and "timeperevent" in s.value:
          del s.value["timeperevent"]
        s.value["resettimeperevent"] = True
