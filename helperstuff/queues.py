import collections

from utilities import restful

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
      print "Please delete the following bad prepids:"
      for prepid in sorted(self.badprepids):
        print " ", prepid
