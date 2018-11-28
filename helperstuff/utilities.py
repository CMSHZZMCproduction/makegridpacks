import abc, collections, contextlib, errno, functools, getpass, itertools, json, logging, os, re, shutil, subprocess, sys, tempfile, time, urllib

def mkdir_p(path):
  """http://stackoverflow.com/a/600612/5228524"""
  try:
    os.makedirs(path)
  except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(path):
      pass
    else:
      raise

def rm_f(path):
  try:
    os.remove(path)
  except OSError as exc:
    if exc.errno == errno.ENOENT:
      pass
    else:
      raise

@contextlib.contextmanager
def cd(newdir):
  """http://stackoverflow.com/a/24176022/5228524"""
  prevdir = os.getcwd()
  os.chdir(os.path.expanduser(newdir))
  try:
    yield
  finally:
    os.chdir(prevdir)

class TFile(object):
  def __init__(self, *args, **kwargs):
    self.args, self.kwargs = args, kwargs
  def __enter__(self):
    import ROOT
    self.__tfile = ROOT.TFile.Open(*self.args, **self.kwargs)
    return self.__tfile
  def __exit__(self, *err):
    self.__tfile.Close()

def tempfilewrapper(function):
  @functools.wraps(function)
  def newfunction(**kwargs):
    if "dir" not in kwargs:
      from jobsubmission import jobid, jobtype
      if jobtype() == "LSF":
        kwargs["dir"] = "/pool/lsf/{}/{}/".format(getpass.getuser(), jobid())
    return function(**kwargs)
  return newfunction

mkdtemp = tempfilewrapper(tempfile.mkdtemp)
NamedTemporaryFile = tempfilewrapper(tempfile.NamedTemporaryFile)

@contextlib.contextmanager
def cdtemp(**kwargs):
  deleteafter = kwargs.pop("deleteafter", True)
  tmpdir = mkdtemp(**kwargs)
  try:
    with cd(tmpdir):
      yield tmpdir
  finally:
    if deleteafter:
      shutil.rmtree(tmpdir)

class KeepWhileOpenFile(object):
  def __init__(self, name, message=None, deleteifjobdied=False):
    from jobsubmission import jobid
    logging.debug("creating KeepWhileOpenFile {}".format(name))
    self.filename = name
    if message is None: message = jobid()
    self.__message = message
    self.pwd = os.getcwd()
    self.fd = self.f = None
    self.bool = False
    self.deleteifjobdied = deleteifjobdied

  def __enter__(self):
    logging.debug("entering KeepWhileOpenFile {}".format(self.filename))
    with cd(self.pwd):
      logging.debug("does it exist? {}".format(os.path.exists(self.filename)))
      try:
        logging.debug("trying to open")
        self.fd = os.open(self.filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
      except OSError:
        logging.debug("failed: it already exists")
        if self.deleteifjobdied and self.jobdied():
          logging.debug("but the job died")
          try:
            with cd(self.pwd):
              logging.debug("trying to remove")
              os.remove(self.filename)
              logging.debug("removed")
          except OSError:
            logging.debug("failed")
            pass #ignore it

        return None
      else:
        logging.debug("succeeded: it didn't exist")
        logging.debug("does it now? {}".format(os.path.exists(self.filename)))
        if not os.path.exists(self.filename):
          logging.warning("{} doesn't exist!??".format(self.filename))
        self.f = os.fdopen(self.fd, 'w')
        try:
          if self.__message:
            logging.debug("writing message")
            self.f.write(self.__message+"\n")
            logging.debug("wrote message")
        except IOError:
          logging.debug("failed to write message")
          pass
        try:
          logging.debug("trying to close")
          self.f.close()
          logging.debug("closed")
        except IOError:
          logging.debug("failed to close")
          pass
        self.bool = True
        return True

  def __exit__(self, *args):
    logging.debug("exiting")
    if self:
      try:
        with cd(self.pwd):
          logging.debug("trying to remove")
          os.remove(self.filename)
          logging.debug("removed")
      except OSError:
        logging.debug("failed")
        pass #ignore it
    self.fd = self.f = None
    self.bool = False

  def __nonzero__(self):
    return self.bool

  def jobdied(self):
    try:
      with open(self.filename) as f:
        try:
          jobid = int(f.read().strip())
        except ValueError:
          return False
        return jobended(str(jobid))
    except IOError:
      return False

class OneAtATime(KeepWhileOpenFile):
  def __init__(self, name, delay, message=None, task="doing this", kwofmessage=None):
    super(OneAtATime, self).__init__(name, message=kwofmessage)
    self.delay = delay
    if message is None:
      message = "Another process is already {task}!  Waiting {delay} seconds."
    message = message.format(delay=delay, task=task)
    self.__message = message

  def __enter__(self):
    while True:
      result = super(OneAtATime, self).__enter__()
      if result:
        return result
      print self.__message
      time.sleep(self.delay)

def cache(function):
  cache = {}
  @functools.wraps(function)
  def newfunction(*args, **kwargs):
    try:
      return cache[args, tuple(sorted(kwargs.iteritems()))]
    except TypeError:
      print args, tuple(sorted(kwargs.iteritems()))
      raise
    except KeyError:
      cache[args, tuple(sorted(kwargs.iteritems()))] = function(*args, **kwargs)
      return newfunction(*args, **kwargs)
  return newfunction

def cacheaslist(function):
  @cache
  @functools.wraps(function)
  def newfunction(*args, **kwargs):
    return list(function(*args, **kwargs))
  return newfunction

def wget(url, output=None):
  if output is None: output = os.path.basename(url)
  with contextlib.closing(urllib.urlopen(url)) as f, open(output, "w") as newf:
    newf.write(f.read())

class JsonDict(object):
  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def keys(self): pass

  @property
  def default(self):
    return JsonDict.__nodefault

  @abc.abstractmethod
  def dictfile(self):
    """should be a member, not a method"""





  __nodefault = object()
  __dictscache = collections.defaultdict(lambda: None)

  def setvalue(self, value):
    self.setnesteddictvalue(self.getdict(), *self.keys, value=value)
    assert self.value == value

  def getvalue(self):
    try:
      return self.getnesteddictvalue(self.getdict(), *self.keys, default=self.default)
    except:
      print "Error while getting value of\n{!r}".format(self)
      raise

  def delvalue(self):
    self.delnesteddictvalue(self.getdict(), *self.keys)

  @property
  def value(self):
    return self.getvalue()

  @value.setter
  def value(self, value):
    self.setvalue(value)

  @value.deleter
  def value(self):
    self.delvalue()

  @classmethod
  def getdict(cls, trycache=True, usekwof=True):
    if cls.__dictscache[cls] is None or not trycache:
      with OneAtATime(cls.dictfile+".tmp", 5, task="accessing the dict for {}".format(cls.__name__)) if usekwof else nullcontext():
        try:
          with open(cls.dictfile) as f:
            jsonstring = f.read()
        except IOError:
          try:
            os.makedirs(os.path.dirname(cls.dictfile))
          except OSError:
            pass
          with open(cls.dictfile, "w") as f:
            f.write("{}\n")
            jsonstring = "{}"
        cls.__dictscache[cls] = json.loads(jsonstring)
    return cls.__dictscache[cls]

  @classmethod
  def writedict(cls):
    dct = cls.getdict()
    jsonstring = json.dumps(dct, sort_keys=True, indent=4, separators=(',', ': '))
    with open(cls.dictfile, "w") as f:
      f.write(jsonstring)

  @classmethod
  def getnesteddictvalue(cls, thedict, *keys, **kwargs):
    hasdefault = False
    for kw, kwarg in kwargs.iteritems():
      if kw == "default":
        if kwarg is not JsonDict.__nodefault:
          hasdefault = True
          default = kwarg
      else:
        raise TypeError("Unknown kwarg {}={}".format(kw, kwarg))

    if len(keys) == 0:
      return thedict

    if hasdefault and keys[0] not in thedict:
      if len(keys) == 1:
        thedict[keys[0]] = default
      else:
        thedict[keys[0]] = {}

    return cls.getnesteddictvalue(thedict[keys[0]], *keys[1:], **kwargs)

  @classmethod
  def setnesteddictvalue(cls, thedict, *keys, **kwargs):
    for kw, kwarg in kwargs.iteritems():
      if kw == "value":
        value = kwarg
      else:
        raise TypeError("Unknown kwarg {}={}".format(kw, kwarg))

    try:
      value
    except NameError:
      raise TypeError("Didn't provide value kwarg!")

    if len(keys) == 1:
      thedict[keys[0]] = value
      return

    if keys[0] not in thedict:
      thedict[keys[0]] = {}

    return cls.setnesteddictvalue(thedict[keys[0]], *keys[1:], **kwargs)

  @classmethod
  def delnesteddictvalue(cls, thedict, *keys, **kwargs):
    if len(keys) == 1:
      del thedict[keys[0]]
      return

    cls.delnesteddictvalue(thedict[keys[0]], *keys[1:], **kwargs)

    if not thedict[keys[0]]: del thedict[keys[0]]

  @classmethod
  @contextlib.contextmanager
  def writingdict(cls):
    with OneAtATime(cls.dictfile+".tmp", 5, task="accessing the dict for {}".format(cls.__name__)):
      cls.getdict(trycache=False, usekwof=False)
      try:
        yield
      finally:
        cls.writedict()

@contextlib.contextmanager
def nullcontext(): yield

def jobended(*bjobsargs):
  try:
    bjobsout = subprocess.check_output(["bjobs"]+list(bjobsargs), stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError:
    return True
  if re.match("Job <.*> is not found", bjobsout.strip()):
    return True
  lines = bjobsout.strip().split("\n")
  if len(lines) == 2 and lines[1].split()[2] in ("EXIT", "DONE"):
    return True

  return False

@contextlib.contextmanager
def redirect_stdout(target):
  original = sys.stdout
  sys.stdout = target
  try:
    yield
  finally:
    sys.stdout = original

def restful(*args, **kwargs):
  if "dev" not in kwargs: kwargs["dev"] = False
  try:
    with open("/dev/null", "w") as f, redirect_stdout(f):
      return rest.McM(*args, **kwargs)
  except:
    raise

here = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
#do not change these once you've started making tarballs!
#they are included in the tarball name and the script
#will think the tarballs don't exist even if they do
cmsswversion = "CMSSW_9_3_0"
scramarch = "slc6_amd64_gcc630"

genproductions = os.path.join(os.environ["CMSSW_BASE"], "src", "genproductions")
if not os.path.exists(genproductions) or os.environ["CMSSW_VERSION"] != cmsswversion or os.environ["SCRAM_ARCH"] != scramarch:
  raise RuntimeError("Need to cmsenv in a " + cmsswversion + " " + scramarch + " release that contains genproductions")

def recursivesubclasses(cls):
  result = [cls]
  for subcls in cls.__subclasses__():
    for subsubcls in recursivesubclasses(subcls):
      if subsubcls not in result:
        result.append(subsubcls)
  return result

@cache
def makecards(folder):
  with cd(folder):
    subprocess.check_call(["./makecards.py"])

class OrderedCounter(collections.Counter, collections.OrderedDict):
  pass

if os.path.exists(os.path.join(here, "helperstuff", "rest.pyc")):
  os.remove(os.path.join(here, "helperstuff", "rest.pyc"))

assert not os.path.exists(os.path.join(here, "helperstuff", "rest.pyc"))

class KeyDefaultDict(collections.defaultdict):
  """
  http://stackoverflow.com/a/2912455
  """
  def __missing__(self, key):
    if self.default_factory is None:
      raise KeyError( key )
    else:
      ret = self[key] = self.default_factory(key)
      return ret

from jobsubmission import jobtype

if not jobtype():
  sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
  import rest

@cache
def fullinfo(prepid):
  result = restful().get("requests", query="prepid="+prepid)
  if not result:
    raise ValueError("mcm query for prepid="+prepid+" returned None!")
  if len(result) == 0:
    raise ValueError("mcm query for prepid="+prepid+" returned nothing!")
  if len(result) > 1:
    raise ValueError("mcm query for prepid="+prepid+" returned multiple results!")
  return result[0]
