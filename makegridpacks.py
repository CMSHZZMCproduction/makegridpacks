#!/usr/bin/env python

import os, sys, urllib

from helperstuff.requestqueue import RequestQueue
from helperstuff import allsamples

def makegridpacks():
  with RequestQueue() as queue:
    for sample in allsamples():
      print sample, sample.makegridpack(queue)
      sys.stdout.flush()

if __name__ == "__main__":
  makegridpacks()
