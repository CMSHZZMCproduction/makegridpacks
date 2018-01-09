#!/usr/bin/env python

raise RuntimeError("I think this script might break mcm.  Don't run it")

import argparse
import pprint

from helperstuff import allsamples
from helperstuff.utilities import restful

def maketicket(block, chain, tags, filter=lambda sample: True, modifyticket=None, notes=None, dryrun=False):
  prepids = [sample.prepid for sample in allsamples(filter=lambda sample: sample.prepid is not None and filter(sample))]
  if not prepids:
    print "no prepids!"
    return
  prepids.sort()
  firstpart = {prepid.rsplit("-", 1)[0] for prepid in prepids}
  if len(firstpart) > 1:
    raise ValueError("The prepids have to be all the same except for the final number, but there are multiple different ones:\n"+", ".join(firstpart))
  firstpart = firstpart.pop()

  if modifyticket is not None:
    ticket = restful().getA("mccms", modifyticket)
  else:
    ticket = {
      "prepid": firstpart.split("-")[0],
      "pwg": firstpart.split("-")[0],
    }

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

  answer = (restful().updateA if modifyticket else restful().putA)('mccms', ticket)
  print answer

  if not answer['results']:
    raise RuntimeError(answer)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--block", "-b", required=True, type=int)
  parser.add_argument("--chain", "-c", required=True)
  parser.add_argument("--filter", "-f", type=eval, default=lambda sample: True)
  parser.add_argument("--modify", "-m")
  parser.add_argument("--notes")
  parser.add_argument("--tags", "-t", action="append", required=True)
  parser.add_argument("--dry-run", "-n", action="store_true")
  args = parser.parse_args()
  maketicket(block=args.block, chain=args.chain, filter=args.filter, modifyticket=args.modify, notes=args.notes, tags=args.tags, dryrun=args.dry_run)
