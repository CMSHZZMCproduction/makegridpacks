import collections, contextlib, csv, os, subprocess

from utilities import cd, LSB_JOBID, NamedTemporaryFile, restful

class RequestQueue(object):
  def __init__(self, dryrun):
    self.dryrun = dryrun
  def __enter__(self):
    self.csvlines = []
    self.requests = []
    self.requeststoapprove = collections.defaultdict(list)
    self.nLSFjobs = 0
    return self
  def addrequest(self, request, **kwargs):
    if request.prepid is not None and not kwargs.get("useprepid"):
      raise RuntimeError("Request {} is already made!".format(request))
    self.csvlines.append(request.csvline(**kwargs))
    if not os.path.exists(os.path.expanduser("~/private/prod-cookie.txt")):
      raise RuntimeError("Have to run\n  source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh\nprior to doing cmsenv")
    self.requests.append(request)
  def validate(self, request):
    self.requeststoapprove[1].append(request)
  def define(self, request):
    self.requeststoapprove[2].append(request)
  def reset(self, request):
    self.requeststoapprove[0].append(request)
  def submitLSF(self, request):
    self.nLSFjobs += 1
  def __exit__(self, *errorstuff):
    if LSB_JOBID(): return

    print
    if self.dryrun:
      print "would submit {} jobs to the queue".format(self.nLSFjobs)
    else:
      print "submitting {} jobs to the queue".format(self.nLSFjobs)
      for i in range(self.nLSFjobs):
        job = "cd "+os.getcwd()+" && eval $(scram ru -sh) && ./makegridpacks.py"
        pipe = subprocess.Popen(["echo", job], stdout=subprocess.PIPE)
        subprocess.check_call(["bsub", "-q", "1nd", "-J", "makegridpacks_{}".format(i)], stdin=pipe.stdout)

    print
    print "modifying requests on McM"
    if self.dryrun:
      print "(would send {} requests)".format(len(self.csvlines))
    else:
      keylists = {frozenset(line.keys()) for line in self.csvlines}
      for keys in keylists:
        with contextlib.closing(NamedTemporaryFile(bufsize=0)) as f:
          writer = csv.DictWriter(f, keys)
          writer.writeheader()
          for line in self.csvlines:
            if frozenset(line.keys()) == keys:
              writer.writerow(line)
          try:
            command = ["McMScripts/manageRequests.py", "--pwg", "HIG", "-c", "RunIIFall17wmLHEGS", f.name]
            if "prepid" in keys: command.append("-m")
            output = subprocess.check_output(command)
          except subprocess.CalledProcessError as e:
            output = e.output
            raise
          except:
            output = ""
          finally:
            print output,
          if "failed to be created" in output or "failed to be modified" in output:
            raise RuntimeError("Failed to create/modify request")
      for request in self.requests:
        request.needsupdate = False
        request.resettimeperevent = False
      del self.csvlines[:], self.requests[:]

    print
    print "approving and resetting requests on McM"
    for level, requests in self.requeststoapprove.iteritems():
      for request in requests:
        if self.dryrun:
          print "Would", approveverb(level, ing=False), request, "({})".format(request.prepid)
        else:
          print approveverb(level), request, "({})".format(request.prepid)
          restful().approve("requests", request.prepid, level)
    self.requeststoapprove.clear()

def approveverb(level, ing=True):
  if ing:
    if level == 0: return "resetting"
    if level == 1: return "validating"
    if level == 2: return "defining"
  else:
    if level == 0: return "reset"
    if level == 1: return "validate"
    if level == 2: return "define"
  assert False, level
