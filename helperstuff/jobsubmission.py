import abc, os

def jobtype():
  result = set()

  if "_CONDOR_SCRATCH_DIR" in os.environ: result.add("condor")
  if "LSB_JOBID" in os.environ: result.add("LSF")

  if not result: return None
  if len(result) == 1: return result.pop()
  assert False, result

def CONDOR_setup(jobid, flavor, time):
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
  if jobtype() == "LSF": return os.environ["LSB_JOBID"]
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

class JobQueue(JobTimeBase):
  def __init__(self, queue):
    self.queue = queue
  def __str__(self):
    return self.queue
  @classmethod
  def condortypes(self):
    return {
      "espresso":      datetime.timedelta(minutes=20),
      "microcentury":  datetime.timedelta(hours=1),
      "longlunch":     datetime.timedelta(hours=2),
      "workday":       datetime.timedelta(hours=8),
      "tomorrow":      datetime.timedelta(days=1),
      "testmatch":     datetime.timedelta(days=3),
      "nextweek":      datetime.timedelta(weeks=1),
    }
  @classmethod
  def LSFtypes(self):
    return {
      "8nm": datetime.timedelta(minutes=8),
      "1nh": datetime.timedelta(hours=1),
      "8nh": datetime.timedelta(hours=8),
      "1nd": datetime.timedelta(days=1),
      "2nd": datetime.timedelta(days=2),
      "1nw": datetime.timedelta(weeks=1),
      "2nw": datetime.timedelta(weeks=2),
      "cmscaf1nh": datetime.timedelta(hours=1),
      "cmscaf1nd": datetime.timedelta(days=1),
      "cmscaf1nw": datetime.timedelta(weeks=1),
    }
  @property
  def jobtype(self):
    if self.queue in self.LSFtypes(): return "LSF"
    if self.queue in self.condortypes(): return "condor"
    assert False, self.queue
  @property
  def jobtime(self):
    if self.jobtype == "LSF": return self.LSFtypes()[self.queue]
    if self.jobtype == "condor": return self.condortypes()[self.queue]

class JobTime(JobTimeBase):
  def __init__(self, jobtype, jobtime):
    self.__jobtype = jobtype
    self.__jobtime = jobtime
  @property
  def jobtype(self): return self.jobtype
  @property
  def jobtime(self): return self.jobtime

def jobtime():
  if jobtype() is None:
    return None
  if jobtype() == "condor":
    return __condor_jobtime
  if jobtype() == "LSF":
    return JobQueue(os.environ["LSB_QUEUE"])
  assert False, jobtype()

def queuematches(queue):
  if jobtype() is None: return None
  return jobtime() in JobQueue(queue)
