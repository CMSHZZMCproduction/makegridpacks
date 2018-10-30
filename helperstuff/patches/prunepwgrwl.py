#!/usr/bin/env python

import argparse, collections, glob, os, re, shutil, subprocess, sys

from utilities import cache, cdtemp, OrderedCounter

sys.path.append(os.path.join(os.environ["LHAPDF_DATA_PATH"], "..", "..", "lib", "python2.7", "site-packages"))

class AlternateWeight(collections.namedtuple("AlternateWeight", "lhapdf renscfact facscfact")):
  def __new__(cls, lhapdf, renscfact=None, facscfact=None):
    lhapdf = int(lhapdf)
    renscfact = cls.parsescalefactor(renscfact)
    facscfact = cls.parsescalefactor(facscfact)
    return super(AlternateWeight, cls).__new__(cls, lhapdf=lhapdf, renscfact=renscfact, facscfact=facscfact)

  @classmethod
  def parsescalefactor(cls, scalefactor):
    if scalefactor is None: return 1
    return float(scalefactor.replace("d", "e").replace("D", "E"))

  @property
  @cache
  def pdf(self):
    import lhapdf
    return lhapdf.mkPDF(self.lhapdf)

  @property
  def pdfname(self):
    return self.pdf.set().name
  @property
  def pdfmemberid(self):
    return self.pdf.memberID

def prunepwgrwl(oldfilename, newfilename, filter, verbose=False):
  oldfilename = os.path.abspath(oldfilename)
  newfilename = os.path.abspath(newfilename)

  with cdtemp():
    subprocess.check_call(["tar", "xvaf", oldfilename])

    if not os.path.exists("original-pwg-rwl.dat"):
      shutil.move("pwg-rwl.dat", "original-pwg-rwl.dat")

    if verbose:
      keep = OrderedCounter()
      remove = OrderedCounter()

    with open("original-pwg-rwl.dat") as f, open("pwg-rwl.dat", "w") as newf:
      for line in f:
        if "<weight id" in line:
          match = re.match(r"^<weight id='[^']*'>((?:\s*\w*=[\w.]*\s*)*)</weight>$", line.strip())
          if not match: raise ValueError("Bad pwg-rwl line:\n"+line)
          kwargs = dict(_.split("=") for _ in match.group(1).split())
          weight = AlternateWeight(**kwargs)

          if filter and not filter(weight):
            if verbose: remove[weight.pdfname] += 1
            continue
          if verbose: keep[weight.pdfname] += 1

        newf.write(line)

    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

    if verbose:
      print "Keeping", sum(keep.values()), "alternate weights:"
      for name, n in keep.iteritems():
        if n>1: print "   {} ({} variations)".format(name, n)
        else: print "   {}".format(name)
      print
      print "Removing", sum(remove.values()), "alternate weights:"
      for name, n in remove.iteritems():
        if n>1: print "   {} ({} variations)".format(name, n)
        else: print "   {}".format(name)

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  p.add_argument("--filter", required=True, type=eval)
  p.add_argument("-v", "--verbose", action="store_true")
  args = p.parse_args()
  prunepwgrwl(**args.__dict__)
