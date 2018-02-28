import collections

from utilities import restful
from requesturl import requesturl_prepids

class ApprovalQueue(object):
  def __enter__(self):
    self.approvals = collections.defaultdict(set)
    return self
  def approve(self, request, level):
    self.approvals[level].add(request.prepid)
  def reset(self, request): self.approve(request, 0)
  def validate(self, request): self.approve(request, 1)
  def define(self, request): self.approve(request, 2)
  def __exit__(self, *err):
    for level, prepids in self.approvals.iteritems():
      print
      print "approving", len(prepids), "requests to level", level
      for prepid in sorted(prepids): print " ", prepid
      restful().approve("requests", ",".join(prepids), level)

class BadRequestQueue(object):
  def __enter__(self):
    self.badprepids = set()
    return self
  def add(self, request):
    self.badprepids.add(request.badprepid)
    return "please delete the bad prepid {} before proceeding".format(request.badprepid)
  def __exit__(self, *err):
    if self.badprepids:
      print
      print "Please delete the following bad prepids:"
      print " "+requesturl_prepids(self.badprepids)

class CloneQueue(object):
  def __enter__(self):
    self.clones = collections.defaultdict(set)
    return self
  def add(self, request, newpwg, newcampaign):
    self.clones[newpwg+"-"+newcampaign].add(request.originalprepid)
    return "please clone the request (see below)"
  def __exit__(self, *err):
    for newpwgcampaign, clones in self.clones.iteritems():
      print
      print "Please clone the following prepids into "+newpwgcampaign+":"
      print " "+requesturl_prepids(clones)
