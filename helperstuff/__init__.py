def allsamples(filter=lambda sample: True, onlymysamples=True, __docheck=True):
  import getpass
  from utilities import recursivesubclasses
  from mcsamplebase import MCSampleBase

  #import all modules that have classes that should be considered here
  import jhugenjhugenanomalouscouplings, jhugenjhugenmassscanmcsample, \
         powhegjhugenanomalouscouplings, powhegjhugenmassscanmcsample, powhegjhugenlifetime, \
         minlomcsample, mcfmanomalouscouplings, pythiavariationsample, phantommcsample, \
         qqZZmcsample, clonedrequest, gridpackbysomeoneelse

  if __docheck: __checkforduplicates()

  for subcls in recursivesubclasses(MCSampleBase):
    if "allsamples" in subcls.__abstractmethods__: continue
    for sample in subcls.allsamples():
  #    print sample
      if filter(sample) and (not onlymysamples or sample.responsible == getpass.getuser()):
        yield sample

from utilities import cache
from collections import Counter
@cache
def __checkforduplicates():
  bad = set()
  for k, v in Counter(s.keys for s in allsamples(onlymysamples=False, __docheck=False)).iteritems():
    if v > 1:
      bad.add(", ".join(str(_) for _ in k))
  if bad:
    raise ValueError("Multiple samples with these identifiers:\n" + "\n".join(bad))
