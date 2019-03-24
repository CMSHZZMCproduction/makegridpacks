#!/usr/bin/env python

import argparse, logging, os, sys

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--filter", type=eval, help='example: lambda x: hasattr(x, "productionmode") and x.productionmode == "ggH"', default=lambda x: True)
  g = parser.add_mutually_exclusive_group()
  g.add_argument("--suppressfinished", type=eval, help='example (and default): lambda x: True', default='lambda x: True')
  g.add_argument("--dontsuppressfinished", action="store_const", dest="suppressfinished", const=lambda x: False)
  parser.add_argument("--cprofile", action="store_true")
  parser.add_argument("--setneedsupdate", action="store_true")
  parser.add_argument("--condorjobid", help=argparse.SUPPRESS)
  parser.add_argument("--condorjobflavor", help=argparse.SUPPRESS)
  parser.add_argument("--condorjobtime", help=argparse.SUPPRESS)
  parser.add_argument("--disable-duplicate-check", "--disableduplicatecheck", action="store_true", help="disable the check that ensures multiple samples don't have the same prepid or identifiers")
  parser.add_argument("--show-wrong-cmssw", action="store_true", help="show requests that don't match your CMSSW release (but don't act on them)")
  parser.add_argument("--start-from", type=eval, help="start from the first sample that meets this criterion", default=lambda x: True)
  parser.add_argument("--debug", action="store_true", help="print some debug stuff (but fairly limited)")
  args = parser.parse_args()

from helperstuff import allsamples, disableduplicatecheck
from helperstuff.cleanupgridpacks import cleanupgridpacks
from helperstuff.queues import ApprovalQueue, BadRequestQueue, CloneQueue

def makegridpacks(args):
  args.debug = True
  if args.debug: logging.getLogger().setLevel(logging.DEBUG)

  from helperstuff.jobsubmission import condorsetup
  from helperstuff.utilities import cmsswversion, scramarch
  condorsetup(args.condorjobid, args.condorjobflavor, args.condorjobtime)

  if args.disable_duplicate_check: disableduplicatecheck()

  with ApprovalQueue() as approvalqueue, BadRequestQueue() as badrequestqueue, CloneQueue() as clonequeue:
    startprinting = False
    for sample in allsamples(filter=args.filter):
      if args.suppressfinished(sample) and sample.finished: continue
      if not args.show_wrong_cmssw and (sample.cmsswversion, sample.scramarch) != (cmsswversion, scramarch): continue
      if args.start_from(sample): startprinting = True
      if not startprinting: continue
      print sample, sample.makegridpack(approvalqueue, badrequestqueue, clonequeue, setneedsupdate=args.setneedsupdate)
      sys.stdout.flush()

if __name__ == "__main__":
  if args.cprofile:
    import cProfile
    cProfile.run("makegridpacks(args)")
    #results for bbb42613d781d40e6f848a8727cd70c2e6357342 are in data/cprofileresults.txt
  else:
    makegridpacks(args)
