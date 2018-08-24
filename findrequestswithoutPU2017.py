#!/usr/bin/env python

import argparse, getpass, re, sys
parser = argparse.ArgumentParser()
parser.add_argument("--user", default=getpass.getuser())
parser.add_argument("-v", "--verbose", action="store_true")
args = parser.parse_args()

sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
import rest

mcm = rest.restful()

requests = mcm.get("requests", query="prepid=HIG-RunIIFall17wmLHEGS-*&actor="+args.user)

firstchain = "HIG-chain_RunIIFall17wmLHEGS_flowRunIIFall17DRPremix_flowRunIIFall17MiniAODv2_flowRunIIFall17NanoAOD-[0-9]*"
secondchain = "HIG-chain_RunIIFall17wmLHEGS_flowRunIIFall17DRPremixPU2017_flowRunIIFall17MiniAODv2_flowRunIIFall17NanoAOD-[0-9]*"

for req in requests:
  if not req["member_of_chain"]: continue
  chains = [_ for _ in req["member_of_chain"] if "NanoAOD" in _]
  chains.sort(reverse=True)
  if len(chains) == 2 and re.match(firstchain, chains[0]) and re.match(secondchain, chains[1]):
    print "good:", req["prepid"]
  else:
    print "bad: ", req["prepid"]
    if args.verbose:
      if len(chains) == 1:
        print " ", "only one chain"
        print " ", chains[0]
      else:
        print " ", chains
        print " ", re.match(firstchain, chains[0])
        print " ", re.match(secondchain, chains[1])
