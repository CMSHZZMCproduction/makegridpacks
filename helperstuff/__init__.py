def allsamples(filter=lambda sample: True, onlymysamples=True):
  import getpass
  from utilities import recursivesubclasses
  from mcsamplebase import MCSampleBase

  #import all modules that have classes that should be considered here
  import jhugenjhugenanomalouscouplings, jhugenjhugenmassscanmcsample, \
         powhegjhugenanomalouscouplings, powhegjhugenmassscanmcsample, powhegjhugenlifetime, \
         minlomcsample, mcfmanomalouscouplings, pythiavariationsample

  for subcls in recursivesubclasses(MCSampleBase):
    if "allsamples" in subcls.__abstractmethods__: continue
    for sample in subcls.allsamples():
      if filter(sample) and (not onlymysamples or sample.responsible == getpass.getuser()):
        yield sample
