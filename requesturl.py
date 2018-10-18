#!/usr/bin/env python

import argparse
import pprint

from helperstuff import allsamples
from helperstuff.requesturl import requesturl_prepids

allstatuses = ("new", "validation", "defined", "approved", "submitted", "done")

def requesturl(filter=lambda sample: True, status=("defined",), onlymysamples=False):
  prepids = [sample.prepid for sample in allsamples(
    filter=lambda sample:
      sample.prepid is not None
      and filter(sample)
      and (status==allstatuses or sample.status in status),
    onlymysamples=onlymysamples,
  )]
  if not prepids:
    print "no prepids!"
    return
  return requesturl_prepids(prepids)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--filter", "-f", type=eval, default=lambda sample: True)
  group = parser.add_mutually_exclusive_group()
  group.add_argument("--submitted", action="store_true", help="make a ticket using requests that are already approved or submitted")
  group.add_argument("--unvalidated", action="store_true", help="make a ticket using requests that are not validated (status new)")
  group.add_argument("--status", action="append", help="make a ticket using requests that have the statuses listed here", choices=allstatuses)
  group.add_argument("--all-statuses", action="store_true")
  parser.add_argument("--everyones-samples", action="store_true", help="include samples that you are not the responsible person for")
  args = parser.parse_args()

  if args.submitted: status = ("approved", "submitted", "done")
  elif args.unvalidated: status = ("new",)
  elif args.status: status = args.status
  elif args.all_statuses: status = allstatuses
  else: status = ("defined",)

  print requesturl(filter=args.filter, status=status, onlymysamples=not args.everyones_samples)
