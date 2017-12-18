def allsamples(filter=lambda sample: True):
  from mcsamplebase import MCSampleBase
  from utilities import recursivesubclasses

  #import all modules that have classes that should be considered here
  import jhugenjhugenmassscanmcsample, powhegjhugenmassscanmcsample

  for subcls in recursivesubclasses(MCSampleBase):
    if "allsamples" in subcls.__abstractmethods__: continue
    for sample in subcls.allsamples():
      if filter(sample):
        yield sample
