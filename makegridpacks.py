#!/usr/bin/env python

import argparse, os, sys, urllib

from helperstuff import allsamples
from helperstuff.queues import ApprovalQueue, BadRequestQueue

def makegridpacks():
  with ApprovalQueue() as approvalqueue, BadRequestQueue() as badrequestqueue:
    for sample in allsamples():
      print sample, sample.makegridpack(approvalqueue, badrequestqueue)
      sys.stdout.flush()

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  args = parser.parse_args()
  makegridpacks()
