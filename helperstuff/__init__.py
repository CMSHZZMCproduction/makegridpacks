def allsamples():
  from mcsamplebase import MCSampleBase
  from utilities import recursivesubclasses

  #import all modules that have classes that should be considered here
  import powhegjhugenmassscanmcsample

  for subcls in recursivesubclasses(MCSampleBase):
    if subcls.__abstractmethods__: continue
    for sample in subcls.allsamples():
      yield sample
