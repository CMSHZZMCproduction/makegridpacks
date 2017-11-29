#!/usr/bin/env python

import collections, itertools, operator
from makegridpacks import *


if __name__ == "__main__":
  print
  print 'Go to each of the following urls, check the "check all" box at the bottom,'
  print 'then click the "next step" button.'
  print
  prepids = collections.defaultdict(set)
  for productionmode in "ggH", "VBF", "WplusH", "WminusH", "ZH", "ttH":
    for decaymode in "4l", "2l2nu", "2l2q":
      for mass in getmasses(productionmode, decaymode):
        sample = MCSample(productionmode, decaymode, mass)
        if sample.sizeperevent and sample.timeperevent and not sample.needsupdate:
          prepidstart, prepidint = sample.prepid.rsplit("-", 1)
          prepids[prepidstart].add(prepidint)

  for prepidstart, prepidints in prepids.iteritems():
    prepidints = sorted(prepidints)
    for k, g in itertools.groupby(enumerate(prepidints), (lambda (i, x): i-int(x))):
      ints = map(operator.itemgetter(1), g)
      if len(ints) == 1:
        print "https://cms-pdmv.cern.ch/mcm/requests?prepid={}-{}&page=-1&approval=none".format(prepidstart, ints[0])
      else:
        print "https://cms-pdmv.cern.ch/mcm/requests?range={0}-{1},{0}-{2}&page=-1&approval=none".format(prepidstart, ints[0], ints[-1])
  print
