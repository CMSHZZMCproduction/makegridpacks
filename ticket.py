#!/usr/bin/env python

import argparse, itertools, math, pprint

from helperstuff import allsamples
from helperstuff.rest import McM
from helperstuff.utilities import request_fragment_check

def maketicket(block, chain, tags, year, filter=lambda sample: True, modifytickets=[], notes=None, dryrun=False, status=("defined",), onlymysamples=False):
  prepids = [sample.prepid for sample in allsamples(
    filter=lambda sample:
      not sample.finished
      and sample.prepid is not None
      and sample.year == year
      and filter(sample)
      and sample.status in status,
    onlymysamples=onlymysamples,
  )]
  if not prepids:
    print "no prepids!"
    return
  prepids.sort()
  firsttwoparts = {prepid.rsplit("-", 1)[0] for prepid in prepids}
  if len(firsttwoparts) > 1:
    raise ValueError("The prepids have to be all the same except for the final number, but there are multiple different ones:\n"+", ".join(firsttwoparts))
  firsttwoparts = firsttwoparts.pop()

  firstpart, middlepart = firsttwoparts.split("-")
  chainfirstpart = chain.split("_")[1]
  if middlepart != chainfirstpart:
    raise ValueError("Chain first part "+chainfirstpart+" is not the same as the request prepids' middle part "+middlepart)

  ntickets = int(math.ceil(len(prepids) / 40.))
  ineachticket = int(math.ceil(1.*len(prepids) / ntickets))

  if len(modifytickets) > ntickets:
    raise ValueError("Too many tickets to modify, there are only enough requests for "+str(ntickets))

  tickets = []
  for i in xrange(ntickets):
    if i < len(modifytickets):
      tickets.append(McM().get("mccms", modifytickets[i]))
    else:
      tickets.append({
        "prepid": firstpart,
        "pwg": firstpart,
      })

  ticketrequests = []
  requests = []
  currentrequestrange = []

  i = 0
  for prepid in prepids:
    if i == 0:
      requests = []
      currentrequestrange = []
      ticketrequests.append(requests)
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
    i += 1
    if i == ineachticket:
      i = 0

  for ticket, requests in itertools.izip(tickets, ticketrequests):
    assert all(1 <= len(lst) <= 2 for lst in requests)
    requests = [lst if len(lst) == 2 else lst[0] for lst in requests]

    ticket.update({
      "block": block,
      "chains": [chain],
      "repetitions": 1,
      "requests": requests,
      "tags": tags,
    })
    if notes is not None: ticket["notes"] = notes

    pprint.pprint(ticket)

  if dryrun: return

  for ticket in tickets:
    answer = (McM().update if modifytickets else McM().put)('mccms', ticket)
    print answer

    if not answer['results']:
      raise RuntimeError(answer)

    if "prepid" not in answer: answer["prepid"] = ticket["prepid"]

    print request_fragment_check("--ticket", answer["prepid"], "--bypass_status")

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--block", "-b", required=True, type=int)
  parser.add_argument("--chain", "-c", required=True)
  parser.add_argument("--filter", "-f", type=eval, default=lambda sample: True)
  parser.add_argument("--year", "-y", type=int, required=True)
  parser.add_argument("--modify", "-m", action="append", default=[])
  parser.add_argument("--notes")
  parser.add_argument("--tags", "-t", action="append", required=True)
  parser.add_argument("--dry-run", "-n", action="store_true")
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

  maketicket(block=args.block, chain=args.chain, filter=args.filter, modifytickets=args.modify, notes=args.notes, tags=args.tags, dryrun=args.dry_run, status=status, onlymysamples=not args.everyones_samples, year=args.year)
