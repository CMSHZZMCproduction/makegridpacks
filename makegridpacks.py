#!/usr/bin/env python

import argparse, os, sys, urllib

from helperstuff import allsamples
from helperstuff.queues import ApprovalQueue, BadRequestQueue

def makegridpacks(args):
  with ApprovalQueue() as approvalqueue, BadRequestQueue() as badrequestqueue:
    for sample in allsamples(filter=args.filter):
      print sample, sample.makegridpack(approvalqueue, badrequestqueue)
      sys.stdout.flush()

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--filter", type=eval, help='example: lambda x: hasattr(x, "productionmode") and x.productionmode == "ggH"')
  args = parser.parse_args()
  makegridpacks(args)
