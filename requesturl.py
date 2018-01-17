#!/usr/bin/env python

import argparse
import pprint

from helperstuff import allsamples
from helperstuff.utilities import restful

def requesturl(filter=lambda sample: True, status=("defined",), onlymysamples=False):
  prepids = [sample.prepid for sample in allsamples(
    filter=lambda sample:
      sample.prepid is not None
      and filter(sample)
      and sample.status in status,
    onlymysamples=onlymysamples,
  )]
  if not prepids:
    print "no prepids!"
    return
  prepids.sort()
  firstpart = {prepid.rsplit("-", 1)[0] for prepid in prepids}
  if len(firstpart) > 1:
    raise ValueError("The prepids have to be all the same except for the final number, but there are multiple different ones:\n"+", ".join(firstpart))
  firstpart = firstpart.pop()

  requests = []
  currentrequestrange = []
  for prepid in prepids:
    if len(currentrequestrange) == 2:
      if int(prepid.split("-")[-1]) == int(currentrequestrange[1].split("-")[-1])+1:
        currentrequestrange[1] = prepid
      else:
        currentrequestrange = []
    if len(currentrequestrange) == 1:
      if int(prepid.split("-")[-1]) == int(currentrequestrange[0].split("-")[-1])+1:
        currentrequestrange.append(prepid)
      else:
        currentrequestrange = []
    if not currentrequestrange:  #not elif!!
      currentrequestrange.append(prepid)
      requests.append(currentrequestrange)
    assert prepid == currentrequestrange[-1]
    assert currentrequestrange is requests[-1]  #is rather than ==, so that we can modify it in place

  return "https://cms-pdmv.cern.ch/mcm/requests?range="+";".join(",".join(requestrange) for requestrange in requests)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--filter", "-f", type=eval, default=lambda sample: True)
  group = parser.add_mutually_exclusive_group()
  group.add_argument("--submitted", action="store_true", help="make a ticket using requests that are already approved or submitted")
  group.add_argument("--unvalidated", action="store_true", help="make a ticket using requests that are not validated (status new)")
  group.add_argument("--status", action="append", help="make a ticket using requests that have the statuses listed here", choices=("new", "validation", "defined", "approved", "submitted", "done"))
  parser.add_argument("--everyones-samples", action="store_true", help="include samples that you are not the responsible person for")
  args = parser.parse_args()

  if args.submitted: status = ("approved", "submitted", "done")
  elif args.unvalidated: status = ("new",)
  elif args.status: status = args.status
  else: status = ("defined",)

  print requesturl(filter=args.filter, status=status, onlymysamples=not args.everyones_samples)
