def allsamples(filter=lambda sample: True, onlymysamples=True, clsfilter=lambda cls: True, includefinished=True):
  import getpass
  from utilities import recursivesubclasses
  from mcsamplebase import MCSampleBase

  #import all modules that have classes that should be considered here
  import jhugenjhugenanomalouscouplings, jhugenjhugenmassscanmcsample, jhugenoffshellVBF, \
         powhegjhugenanomalouscouplings, powhegjhugenmassscanmcsample, powhegjhugenlifetime, \
         minlomcsample, mcfmanomalouscouplings, variationsample, phantommcsample, \
         qqZZmcsample, clonedrequest, gridpackbysomeoneelse, mtdtdr

  __checkforduplicates()

  for subcls in recursivesubclasses(MCSampleBase):
    if "allsamples" in subcls.__abstractmethods__: continue
    if not clsfilter(subcls): continue
    for sample in subcls.allsamples():
      if (not sample.finished or includefinished) and filter(sample) and (not onlymysamples or sample.responsible == getpass.getuser()):
        yield sample

from collections import Counter, defaultdict

def __checkforduplicates():
  global __didcheck
  if __didcheck: return

  from gridpackonly import GridpackOnly

  disableduplicatecheck()
  bad = set()
  identifiers = Counter()
  prepids = Counter()
  datasets = Counter()
  for s in allsamples(onlymysamples=False):
    try:
      if s != type(s)(*s.initargs, **s.initkwargs):
        raise ValueError("s = "+repr(s)+", type(s)(s.initargs) = "+repr(type(s)(*s.initargs)))
    except TypeError:
      print "initargs for", s, "are messed up"
      raise
    identifiers[s.keys] += 1
    if isinstance(s, GridpackOnly): continue
    prepids[s.prepid] += 1
    datasets[s.campaign, s.datasetname, s.extensionnumber] += 1

  for k, v in identifiers.iteritems():
    if v > 1:
      bad.add(", ".join(str(_) for _ in k))
  if bad:
    raise ValueError("Multiple samples with these identifiers:\n" + "\n".join(bad))

  for k, v in prepids.iteritems():
    if k is not None and v > 1:
      bad.add("{} ({})".format(k, v))
  if bad:
    raise ValueError("Multiple samples with these prepids:\n" + "\n".join(bad))

  for k, v in datasets.iteritems():
    if k is not None and v > 1:
      bad.add("{}, {}, {} ({})".format(*k+(v,)))
  if bad:
    raise ValueError("Multiple samples with the same campaign, dataset name, and extension number:\n" + "\n".join(bad))

def disableduplicatecheck():
  global __didcheck
  __didcheck = True

__didcheck = False
