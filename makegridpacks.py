#!/usr/bin/env python

import argparse, os, sys, urllib

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--filter", type=eval, help='example: lambda x: hasattr(x, "productionmode") and x.productionmode == "ggH"', default=lambda x: True)
  g = parser.add_mutually_exclusive_group()
  g.add_argument("--suppressfinished", type=eval, help='example (and default): lambda x: x.year==2017', default='lambda x: x.year==2017')
  g.add_argument("--dontsuppressfinished", action="store_const", dest="suppressfinished", const=lambda x: False)
  parser.add_argument("--cprofile", action="store_true")
  parser.add_argument("--setneedsupdate", action="store_true")
  parser.add_argument("--jobid")
  parser.add_argument("--jobflavor")
  args = parser.parse_args()

from helperstuff import allsamples
from helperstuff.cleanupgridpacks import cleanupgridpacks
from helperstuff.queues import ApprovalQueue, BadRequestQueue, CloneQueue

def makegridpacks(args):
  with ApprovalQueue() as approvalqueue, BadRequestQueue() as badrequestqueue, CloneQueue() as clonequeue:
    for sample in allsamples(filter=args.filter):
      if args.suppressfinished(sample) and sample.finished: continue
      print sample, sample.makegridpack(approvalqueue, badrequestqueue, clonequeue, setneedsupdate=args.setneedsupdate)
      sys.stdout.flush()

if __name__ == "__main__":
  if args.cprofile:
    import cProfile
    cProfile.run("makegridpacks(args)")
    #results for bbb42613d781d40e6f848a8727cd70c2e6357342 are in data/cprofileresults.txt
  else:
    makegridpacks(args)
