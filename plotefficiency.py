#!/usr/bin/env python

import array, ROOT, style

from makegridpacks import *

c = ROOT.TCanvas()

for p in "ZH", "ttH":
  print p
  print
  x = array.array('d', getmasses(p, "4l"))
  y = array.array('d', [MCSample(p, "4l", m).matchefficiency for m in getmasses(p, "4l")])
  ex = array.array('d', [0 for m in x])
  ey = array.array('d', [MCSample(p, "4l", m).matchefficiencyerror for m in getmasses(p, "4l")])
  g = ROOT.TGraphErrors(len(x), x, y, ex, ey)
  g.Draw("AP")
  f = ROOT.TF1("constant", "[0]", min(x), max(x))
  g.Fit(f)
  print
  print "chi2/NDF =", f.GetChisquare()/f.GetNDF()
  print "prob =", f.GetProb()
  print
  print
  print
  for ext in "png", "pdf":
    c.SaveAs("~/www/TEST/"+p+"."+ext)
