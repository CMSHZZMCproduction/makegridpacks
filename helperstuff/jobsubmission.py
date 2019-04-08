import abc, datetime, os, re, subprocess

from utilities import cd, here, NamedTemporaryFile, KeyDefaultDict

def jobtype():
  result = set()

  if "_CONDOR_SCRATCH_DIR" in os.environ: result.add("condor")

  if not result: return None
  if len(result) == 1: return result.pop()
  assert False, result

def condorsetup(jobid, flavor, time):
  global __condor_jobid, __condor_jobtime
  if jobid is flavor is time is None: return

  __condor_jobid = jobid
  __condor_jobtime = set()
  if flavor is not None:
    __condor_jobtime.add(JobQueue(flavor))
  if time is not None:
    __condor_jobtime.add(JobTime("condor", time))
  assert len(__condor_jobtime) == 1, __condor_jobtime
  __condor_jobtime = __condor_jobtime.pop()

def jobid():
  if jobtype() is None: return None
  if jobtype() == "condor": return __condor_jobid
  assert False, jobtype()

class JobTimeBase(object):
  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def jobtype(self): pass
  @abc.abstractproperty
  def jobtime(self): pass

  def __str__(self):
    return "({}, {})".format(self.jobtype, self.jobtime)

  def __cmp__(self, other): return cmp(self.jobtime, other.jobtime)
  def __hash__(self): return hash((JobTimeBase, self.jobtime))

  def __contains__(self, other):
    """
    Intended use:
      if (current jobtime) in (jobtime for this process to happen): do the process
    """
    return self <= other <= 2*self
  def __add__(self, other):
    if self.jobtype != other.jobtype: raise ValueError("Can't add {} + {} with different jobtypes".format(self, other))
    return JobTime(self.jobtype, self.jobtime+other.jobtime)
  def __mul__(self, factor):
    return JobTime(self.jobtype, self.jobtime*factor)
  def __div__(self, factor):
    return JobTime(self.jobtype, self.jobtime/factor)
  def __rmul__(self, factor):
    return self * factor
  @classmethod
  def condorflavors(self):
    return {
      "espresso":      datetime.timedelta(minutes=20),
      "microcentury":  datetime.timedelta(hours=1),
      "longlunch":     datetime.timedelta(hours=2),
      "workday":       datetime.timedelta(hours=8),
      "tomorrow":      datetime.timedelta(days=1),
      "testmatch":     datetime.timedelta(days=3),
      "nextweek":      datetime.timedelta(weeks=1),
    }
  @property
  def condorflavor(self):
    return min(((k, v) for k, v in self.condorflavors().iteritems() if v >= self.jobtime), key=lambda x: list(reversed(x)))[0]

class JobQueue(JobTimeBase):
  def __init__(self, queue):
    self.queue = queue
  def __str__(self):
    return self.queue
  @property
  def jobtype(self):
    if self.queue in self.condorflavors(): return "condor"
    assert False, self.queue
  @property
  def jobtime(self):
    if self.jobtype == "condor": return self.condorflavors()[self.queue]

class JobTime(JobTimeBase):
  def __init__(self, jobtype, jobtime):
    self.__jobtype = jobtype
    self.__jobtime = jobtime
  @property
  def jobtype(self): return self.__jobtype
  @property
  def jobtime(self): return self.__jobtime

def jobtime():
  if jobtype() is None:
    return None
  if jobtype() == "condor":
    return __condor_jobtime
  assert False, jobtype()

def queuematches(queue):
  if jobtype() is None: return None
  return jobtime() in JobQueue(queue)




def submitLSF(queue):
  raise RuntimeError("No more LSF!")

condortemplate = """
executable              = {here}/.makegridpacks_{jobflavor}.sh
arguments               = "{here} --condorjobid $(ClusterId).$(ProcId) --condorjobflavor {jobflavor} --filter '{filter}'"
output                  = CONDOR/$(ClusterId).out
error                   = CONDOR/$(ClusterId).err
log                     = CONDOR/$(ClusterId).log

request_memory          = 2000M
+JobFlavour             = "{jobflavor}"

#https://www-auth.cs.wisc.edu/lists/htcondor-users/2010-September/msg00009.shtml
periodic_remove         = JobStatus == 5

queue
"""

condortemplate_sizeperevent = """
executable            = {self.workdir}/{self.prepid}
output                = {self.workdir}/$(ClusterId).out
error                 = {self.workdir}/$(ClusterId).err
log                   = {self.workdir}/$(ClusterId).log

request_memory        = 4000M
request_cpus          = {self.nthreads}
+JobFlavour           = "{self.timepereventflavor}"

#https://www-auth.cs.wisc.edu/lists/htcondor-users/2010-September/msg00009.shtml
periodic_remove       = JobStatus == 5

transfer_output_files = {self.prepid}_rt.xml

queue
"""

condortemplate_filter = """
executable            = {self.workdir}/$(i)/filterjobscript
output                = {self.workdir}/$(i)/filterjob.out
error                 = {self.workdir}/$(i)/filterjob.err
log                   = {self.workdir}/$(i)/filterjob.log
Initialdir            = {self.workdir}/$(i)

request_memory        = 4000M
request_cpus          = {self.nthreadsforfilter}
+JobFlavour           = "{self.filterefficiencyflavor}"

#https://www-auth.cs.wisc.edu/lists/htcondor-users/2010-September/msg00009.shtml
periodic_remove       = JobStatus == 5

transfer_output_files = {self.filterresultsfile}

queue i in {jobstoqueue}
"""

def submitcondor(flavor, sample, writejobid=None):
  if writejobid is not None and os.path.exists(writejobid):
    raise RuntimeError(writejobid + " already exists")
  flavor = JobQueue(flavor).condorflavor
  if __pendingjobsdct[flavor] > 0:
    __pendingjobsdct[flavor] -= 1
    return False
  with cd(here), NamedTemporaryFile(bufsize=0) as f:
    f.write(condortemplate.format(
      jobflavor=flavor,
      here=here,
      filter="lambda x: x.identifiers == (" + ", ".join(repr(i).replace("'", "''").replace('"', '""') for i in sample.identifiers)+")"
    ))
    output = subprocess.check_output(["condor_submit", f.name])
    match = re.search("1 job[(]s[)] submitted to cluster ([0-9]*)[.]", output)
    if not match: raise ValueError("didn't match??\n\n"+output+"\n\n")
    print output,
    outputjobid = match.group(1) + ".0"
    if writejobid is not None:
      with open(writejobid, "w") as f:
        f.write(outputjobid)
    return True

def __npendingjobs(queue):
  jobtype = JobQueue(queue).jobtype
  if jobtype == "condor":
    output = subprocess.check_output(["condor_q", "-wide:10000"])
    result = 0
    for line in output.split("\n"):
      line = line.split()
      if len(line) < 3: continue
      if line[2] == ".makegridpacks_{jobflavor}.sh".format(jobflavor=queue):
        result += int(line[7].replace("_", "0"))
    return result
  assert False, jobtype

__pendingjobsdct = KeyDefaultDict(__npendingjobs)
