def allsamples(filter=lambda sample: True, onlymysamples=True, clsfilter=lambda cls: True, __docheck=True, includefinished=True):
  import getpass
  from utilities import recursivesubclasses
  from mcsamplebase import MCSampleBase

  #import all modules that have classes that should be considered here
  import jhugenjhugenanomalouscouplings, jhugenjhugenmassscanmcsample, \
         powhegjhugenanomalouscouplings, powhegjhugenmassscanmcsample, powhegjhugenlifetime, \
         minlomcsample, mcfmanomalouscouplings, variationsample, phantommcsample, \
         qqZZmcsample, clonedrequest, gridpackbysomeoneelse

  if __docheck: __checkforduplicates()

  for subcls in recursivesubclasses(MCSampleBase):
    if "allsamples" in subcls.__abstractmethods__: continue
    if not clsfilter(subcls): continue
    for sample in subcls.allsamples():
      if (not sample.finished or includefinished) and filter(sample) and (not onlymysamples or sample.responsible == getpass.getuser()):
        yield sample

from utilities import cache
from collections import Counter, defaultdict
@cache
def __checkforduplicates():
  bad = set()
  identifiers = Counter()
  prepids = Counter()
  for s in allsamples(onlymysamples=False, __docheck=False):
    identifiers[s.keys] += 1
    prepids[s.prepid] += 1

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
