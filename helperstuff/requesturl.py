#!/usr/bin/env python

import argparse

def requesturl_prepids(prepids):
  prepids = sorted(prepids)
  requests = []
  currentrequestrange = []
  currentfirstpart = ""
  for prepid in prepids:
    if len(currentrequestrange) == 2:
      if prepid.split("-")[:-1] == currentfirstpart and int(prepid.split("-")[-1]) == int(currentrequestrange[1].split("-")[-1])+1:
        currentrequestrange[1] = prepid
      else:
        currentrequestrange = []
    if len(currentrequestrange) == 1:
      if prepid.split("-")[:-1] == currentfirstpart and int(prepid.split("-")[-1]) == int(currentrequestrange[0].split("-")[-1])+1:
        currentrequestrange.append(prepid)
      else:
        currentrequestrange = []
    if not currentrequestrange:  #not elif!!
      currentfirstpart = prepid.split("-")[:-1]
      currentrequestrange.append(prepid)
      requests.append(currentrequestrange)
    assert prepid == currentrequestrange[-1]
    assert currentrequestrange is requests[-1]  #is rather than ==, so that we can modify it in place

  return "https://cms-pdmv.cern.ch/mcm/requests?range="+";".join(",".join(requestrange) for requestrange in requests)
