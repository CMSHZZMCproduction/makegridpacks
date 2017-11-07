import contextlib, errno, functools, itertools, logging, os, tempfile

def mkdir_p(path):
  """http://stackoverflow.com/a/600612/5228524"""
  try:
    os.makedirs(path)
  except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(path):
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

def mkdtemp(**kwargs):
  if "dir" not in kwargs:
    if LSB_JOBID() is not None:
      kwargs["dir"] = "/pool/lsf/hroskes/{}/".format(LSB_JOBID())
  return tempfile.mkdtemp(**kwargs)

def LSB_JOBID():
  return os.environ.get("LSB_JOBID", None)

class KeepWhileOpenFile(object):
  def __init__(self, name, message=None):
    logging.debug("creating KeepWhileOpenFile {}".format(name))
    self.filename = name
    self.__message = message
    self.pwd = os.getcwd()
    self.fd = self.f = None
    self.bool = False

  def __enter__(self):
    logging.debug("entering KeepWhileOpenFile {}".format(self.filename))
    with cd(self.pwd):
      logging.debug("does it exist? {}".format(os.path.exists(self.filename)))
      try:
        logging.debug("trying to open")
        self.fd = os.open(self.filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
      except OSError:
        logging.debug("failed: it already exists")
        return None
      else:
        logging.debug("succeeded: it didn't exist")
        logging.debug("does it now? {}".format(os.path.exists(self.filename)))
        if not os.path.exists(self.filename):
          logging.warning("{} doesn't exist!??".format(self.filename))
        self.f = os.fdopen(self.fd, 'w')
        try:
          if self.__message is not None:
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
