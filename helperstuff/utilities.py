import abc, collections, contextlib, errno, functools, getpass, itertools, json, logging, os, re, shutil, subprocess, sys, tempfile, time, urllib2

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
      except OSError as e:
        if e.errno == errno.EEXIST:
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
          raise
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
    logging.debug("exiting KeepWhileOpenFile {}".format(self.filename))
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
        strjobid = f.read().strip()
        try:
          jobid = float(f.read().strip())
        except ValueError:
          return False
        return jobended(strjobid)
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
  with contextlib.closing(urlopen(url)) as f, open(output, "w") as newf:
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
    jsonstring = json.dumps(dct, sort_keys=True, indent=4, separators=(',', ': ')) + "\n"
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

def jobended(*condorqargs):
  from jobsubmission import jobtype
  if jobtype(): return False  #can't use condor_q on condor machines
  condorqout = subprocess.check_output(["condor_q"]+list(condorqargs), stderr=subprocess.STDOUT)
  match = re.search("([0-9]*) jobs; ([0-9]*) completed, ([0-9]*) removed, ([0-9]*) idle, ([0-9]*) running, ([0-9]*) held, ([0-9]*) suspended", condorqout)
  if not match: raise ValueError("Couldn't parse output of condor_q " + " ".join(condorqargs))
  return all(int(match.group(i)) == 0 for i in xrange(4, 7))

def jobexitstatusfromlog(logfilename, okiffailed=False):
  with open(logfilename) as f:
    log = f.read()

  jobid = re.search("^000 [(]([0-9]*[.][0-9]*)[.][0-9]*[)]", log)
  if not jobid:
    raise RuntimeError("Don't know how to interpret "+logfilename+", can't find the jobid in the file.")
  jobid = jobid.group(1)

  if "Job terminated" in log:
    match = re.search("return value ([0-9]+)", log)
    if match:
      exitstatus = int(match.group(1))
      if exitstatus and not okiffailed:
        raise RuntimeError(logfilename+" failed with exit status {}".format(exitstatus))
      return exitstatus
    else:
      raise RuntimeError("Don't know what to do with "+logfilename)
  if "Job was aborted" in log:
    if okiffailed: return -1
    raise RuntimeError(logfilename+" was aborted")
  return None



@contextlib.contextmanager
def redirect_stdout(target):
  original = sys.stdout
  sys.stdout = target
  try:
    yield
  finally:
    sys.stdout = original

here = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

genproductions = os.path.join(os.environ["CMSSW_BASE"], "src", "genproductions")
if not os.path.exists(genproductions):
  raise RuntimeError("Need to cmsenv in a CMSSW release that contains genproductions")
cmsswversion = os.environ["CMSSW_VERSION"]
scramarch = os.environ["SCRAM_ARCH"]

def osversion():
  try:
    with open("/etc/os-release") as f:
      if 'VERSION_ID="7"' in f.read():
        return 7
  except IOError:
    pass

  try:
    with open("/etc/redhat-release") as f:
      if "Scientific Linux CERN SLC release 6.10 (Carbon)" in f.read():
        return 6
  except IOError:
    pass

  raise RuntimeError("Can't figure out what lxplus version you're on.  Check osversion in utilities.py")

osversion = osversion()

if not scramarch.startswith("slc{:d}".format(osversion)):
  raise RuntimeError("You're on the wrong lxplus.  SCRAM_ARCH={}, you're on {}".format(scramarch, osversion))

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

@cache
def fullinfo(prepid):
  import rest
  result = rest.McM().get("requests", query="prepid="+prepid)
  if not result:
    raise ValueError("mcm query for prepid="+prepid+" returned None!")
  if len(result) == 0:
    raise ValueError("mcm query for prepid="+prepid+" returned nothing!")
  if len(result) > 1:
    raise ValueError("mcm query for prepid="+prepid+" returned multiple results!")
  return result[0]

def urlopen(url, *args, **kwargs):
    try:
        return urllib2.urlopen(url, *args, **kwargs)
    except:
        print "Error while downloading", url
        raise

def createLHEProducer(gridpack, cards, fragment, tag):
    """
    originally from
      https://github.com/davidsheffield/McMScripts
    """

    code = """import FWCore.ParameterSet.Config as cms

externalLHEProducer = cms.EDProducer("ExternalLHEProducer",
    args = cms.vstring('{0}'),
    nEvents = cms.untracked.uint32(5000),
    numberOfParameters = cms.uint32(1),
    outputFile = cms.string('cmsgrid_final.lhe'),
    scriptName = cms.FileInPath('GeneratorInterface/LHEInterface/data/run_generic_tarball_cvmfs.sh')
)""".format(gridpack)

    if cards != "":
        code += """

# Link to cards:
# {0}
""".format(cards)

    if fragment != "" and tag != "":
        gen_fragment_url = "https://raw.githubusercontent.com/cms-sw/genproductions/{0}/{1}".format(
            tag, fragment.split("Configuration/GenProduction/")[1])
        gen_fragment = urlopen(gen_fragment_url).read()
        code += """
{0}

# Link to generator fragment:
# {1}
""".format(gen_fragment, gen_fragment_url)

    return code

def request_fragment_check(*args):
  with cdtemp():
    with contextlib.closing(urlopen("https://github.com/cms-sw/genproductions/raw/master/bin/utils/request_fragment_check.py")) as f:
      contents = f.read()
    with open("request_fragment_check.py", "w") as f:
      f.write(contents)

    return subprocess.check_output(["python", "request_fragment_check.py"] + list(args), stderr=subprocess.STDOUT)

class abstractclassmethod(classmethod):
  "https://stackoverflow.com/a/11218474"
  __isabstractmethod__ = True

  def __init__(self, callable):
    callable.__isabstractmethod__ = True
    super(abstractclassmethod, self).__init__(callable)

#LHAPDF
sys.path.append(os.path.join(os.environ["LHAPDF_DATA_PATH"], "..", "..", "lib", "python2.7", "site-packages"))

@cache
def lhaPDF(PDFnumber):
  import lhapdf
  return lhapdf.mkPDF(PDFnumber)

def PDFname(PDFnumber):
  return lhaPDF(PDFnumber).set().name
def PDFmemberid(PDFnumber):
  return lhaPDF(PDFnumber).memberID
