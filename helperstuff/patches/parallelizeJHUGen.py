#!/usr/bin/env python

import argparse, collections, glob, os, pipes, re, shutil, subprocess, sys

from utilities import cache, cd, cdtemp, OrderedCounter

parallelizationpart = """
if [ ${ncpu} == 1 ]; then
  %(JHUGencommand)s
else
  rm -rf paralleljob_*
  mkdir paralleljob_1
  cp -r * paralleljob_1/
  for i in $(seq 2 ${ncpu}); do
    cp -r paralleljob_1/ paralleljob_${i}/
  done
  seq 1 ${ncpu} | xargs -d "\n" -P${ncpu} -I {} bash -c 'cd paralleljob_{} &&
                                                         rnum=$(('" (${rnum}+${i}) * 12345678 %% 10000000 "'))
                                                         nevt=$(('" ${nevt} * (${i}+1) / ${ncpu} - ${nevt} * ${i} / ${ncpu} "'))  #https://math.stackexchange.com/a/1081099
                                                         %(newJHUGencommand)s'
  (
    cat paralleljob_1/Out.lhe | grep -v "</LesHouchesEvents"
    for i in $(seq 2 $(( ${ncpu} - 1 ))); do
      cat paralleljob_${i}/Out.lhe | sed -e "$(grep -n '<header>' paralleljob_${i}/Out.lhe | sed 's/:.*//'),$(grep -n '</init>' paralleljob_${i}/Out.lhe | sed 's/:.*//')d" | grep -Ev "</?LesHouchesEvents"
    done
    cat paralleljob_${ncpu}/Out.lhe | sed -e "$(grep -n '<header>' paralleljob_${i}/Out.lhe | sed 's/:.*//'),$(grep -n '</init>' paralleljob_${i}/Out.lhe | sed 's/:.*//')d" | grep -Ev "<LesHouchesEvents"
  ) > Out.lhe
  cat paralleljob_*/Out.log > Out.log
fi
"""

def parallelizeJHUGen(oldfilename, newfilename, overwrite=None):
  oldfilename = os.path.abspath(oldfilename)
  newfilename = os.path.abspath(newfilename)

  with cdtemp():
    subprocess.check_call(["tar", "xvaf", oldfilename])

    if not os.path.exists("original_runcmsgrid.sh") or overwrite=="runcmsgrid.sh":
      shutil.move("runcmsgrid.sh", "original_runcmsgrid.sh")
    elif overwrite=="original_runcmsgrid.sh":
      pass
    elif overwrite is not None:
      raise ValueError("overwrite has to be either None, runcmsgrid.sh, or original_runcmsgrid.sh")
    else:
      raise IOError("original_runcmsgrid.sh already exists")

    with open("original_runcmsgrid.sh") as f, open("runcmsgrid.sh", "w") as newf:
      sawJHUGencommand = False
      inJHUGencommand = False
      JHUGencommand = ""
      for line in f:
        if "./JHUGen" in line:
          if sawJHUGencommand:
            raise IOError("Multiple noncontiguous lines with ./JHUGen in runcmsgrid.sh")
          inJHUGencommand = True
          JHUGencommand += line
        elif inJHUGencommand:
          newf.write(parallelizationpart % {"JHUGencommand": JHUGencommand, "newJHUGencommand": JHUGencommand.replace(" ../", " ../../")})
          newf.write(line)
          sawJHUGencommand = True
          inJHUGencommand = False
        else:
          newf.write(line)
        if "parallel" in line:
          raise IOError("runcmsgrid.sh already has parallel")
        if "xargs" in line:
          raise IOError("runcmsgrid.sh already has xargs")
      if not sawJHUGencommand:
        raise IOError("runcmsgrid.sh doesn't have ./JHUGen")
    os.chmod("runcmsgrid.sh", os.stat("original_runcmsgrid.sh").st_mode)

    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  args = p.parse_args()
  parallelizeJHUGen(**args.__dict__)
