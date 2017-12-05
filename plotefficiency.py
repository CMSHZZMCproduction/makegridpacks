#!/usr/bin/env python

import array, ROOT

from makegridpacks import *

c = ROOT.TCanvas()

for p in "ZH", "ttH":
  x = array.array('d', getmasses(p, "4l"))
  y = array.array('d', [MCSample(p, "4l", m).matchefficiency for m in getmasses(p, "4l")])
  ex = array.array('d', [0 for m in x])
  ey = array.array('d', [MCSample(p, "4l", m).matchefficiencyerror for m in getmasses(p, "4l")])
  g = ROOT.TGraphErrors(len(x), x, y, ex, ey)
  g.Draw("AP")
  for ext in "png", "pdf":
    c.SaveAs("~/www/TEST/"+p+"."+ext)
