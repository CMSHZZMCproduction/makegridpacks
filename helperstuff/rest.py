import os

import subprocess

from utilities import cache, here, OneAtATime, redirect_stdout

def maketryagainfunction(name):
  def function(self, *args, **kwargs):
    if self._McM:
      try:
        return getattr(self._McM, name)(*args, **kwargs)
      except ValueError:
        del self._McM
        return function(*args, **kwargs)
    else:
      with OneAtATime(os.path.join(here, "McMlock.tmp"), 5, task="using a cookie for the first time"):
        self.initMcM()
        return getattr(self._McM, name)(*args, **kwargs)
  function.__name__ = name
  return function

class McM(object):
  @cache
  def __new__(cls, *args, **kwargs):
    return super(McM, cls).__new__(cls, *args, **kwargs)
  def __init__(self, *args, **kwargs):
    if not hasattr(self, "_McM"):
      self._McM = None
    self.__McMargs = args
    self.__McMkwargs = kwargs
  def initMcM(self):
    if self._McM is None:
      subprocess.check_call("eval $(scram unsetenv -sh) && bash /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh", shell=True)
      with open("/dev/null", "w") as f, redirect_stdout(f):
        import PdmVrest as rest
        self._McM = rest.McM(*self.__McMargs, **self.__McMkwargs)
  def __getattr__(self, attr):
    self.initMcM()
    return getattr(self._McM, attr)

  get = maketryagainfunction("get")
  update = maketryagainfunction("update")
  put = maketryagainfunction("put")
  approve = maketryagainfunction("approve")
  clone_request = maketryagainfunction("clone_request")
  get_range_of_requests = maketryagainfunction("get_range_of_requests")
  delete = maketryagainfunction("delete")
  forceflow = maketryagainfunction("forceflow")

del maketryagainfunction
