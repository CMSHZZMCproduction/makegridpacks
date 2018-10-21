#!/usr/bin/env python

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument("--set-efficiencies", action="store_true")
  args = parser.parse_args()

import array, ROOT, style

from makegridpacks import *

def makeplots(p, settoaverage=False):
  c = ROOT.TCanvas()

  print p
  print
  x = array.array('d', getmasses(p, "4l"))
  y = array.array('d', [POWHEGJHUGenMassScanMCSample(p, "4l", m).filterefficiency for m in getmasses(p, "4l")])
  ex = array.array('d', [0 for m in x])
  ey = array.array('d', [POWHEGJHUGenMassScanMCSample(p, "4l", m).filterefficiencyerror for m in getmasses(p, "4l")])
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

  if settoaverage:
    for m in getmasses(p, "4l"):
      POWHEGJHUGenMassScanMCSample(p, "4l", m).filterefficiency, POWHEGJHUGenMassScanMCSample(p, "4l", m).filterefficiencyerror = f.GetParameter(0), f.GetParError(0)

if __name__ == "__main__":
  for p in "ZH", "ttH":
    makeplots(p, args.set_efficiencies)
