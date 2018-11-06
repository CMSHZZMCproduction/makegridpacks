#!/usr/bin/env python

import argparse, collections, glob, os, pipes, re, shutil, subprocess, sys

from utilities import cache, cd, cdtemp, OrderedCounter

powhegcommand = '../pwhg_main &> log_${process}_${seed}.txt; test $? -eq 0 || fail_exit "pwhg_main error: exit code not 0"'
assert "'" not in powhegcommand
newpowhegcommand = powhegcommand.replace("../pwhg_main", "../../pwhg_main").replace("fail_exit", "echo").replace('"', "'")

parallelizationpart = """
if [ ${ncpu} == 1 ]; then
  %(powhegcommand)s
else
  rm -rf paralleljob_*
  mkdir paralleljob_1
  cp -r * paralleljob_1/
  for i in $(seq 2 ${ncpu}); do
    cp -r paralleljob_1/ paralleljob_${i}/
  done
  for i in $(seq 1 ${ncpu}); do
    (
      cd paralleljob_${i}
      seed=$(( ($seed+${i}) * 12345678 %% 10000000 ))
      nevt=$(( $nevt * (${i}+1) / ${ncpu} - $nevt * ${i} / ${ncpu} ))  #https://math.stackexchange.com/a/1081099
      cat ${card} | sed -e "s#SEED#${seed}#g" | sed -e "s#NEVENTS#${nevt}#g" > powheg.input
    )
  done
  #seq 1 ${ncpu} | parallel --eta -j${ncpu} "cd paralleljob_{} && %(newpowhegcommand)s"
  #no parallel command on condor
  #https://www.gnu.org/software/parallel/parallel_alternatives.html#DIFFERENCES-BETWEEN-xargs-AND-GNU-Parallel
  seq 1 ${ncpu} | xargs -d "\n" -P${ncpu} -I {} bash -c "cd paralleljob_{} && %(newpowhegcommand)s"
  (
    cat paralleljob_1/pwgevents.lhe | grep -v "</LesHouchesEvents"
    for i in $(seq 2 $(( ${ncpu} - 1 ))); do
      cat paralleljob_${i}/pwgevents.lhe | sed -e "$(grep -n '<header>' paralleljob_${i}/pwgevents.lhe | sed 's/:.*//'),$(grep -n '</init>' paralleljob_${i}/pwgevents.lhe | sed 's/:.*//')d" | grep -Ev "</?LesHouchesEvents"
    done
    cat paralleljob_${ncpu}/pwgevents.lhe | sed -e "$(grep -n '<header>' paralleljob_${i}/pwgevents.lhe | sed 's/:.*//'),$(grep -n '</init>' paralleljob_${i}/pwgevents.lhe | sed 's/:.*//')d" | grep -Ev "<LesHouchesEvents"
  ) > pwgevents.lhe
  cat paralleljob_*/log_${process}_${seed}.txt > log_${process}_${seed}.txt
fi
""" % {"powhegcommand": powhegcommand, "newpowhegcommand": newpowhegcommand}

def parallelizepowheg(oldfilename, newfilename, overwrite=None):
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
      sawpowhegcommand = False
      for line in f:
        if line.rstrip() == powhegcommand:
          if sawpowhegcommand:
            raise IOError("Multiple lines like this in runcmsgrid.sh:\n"+powhegcommand)
          newf.write(parallelizationpart)
          sawpowhegcommand = True
        else:
          newf.write(line)
        if "parallel" in line:
          raise IOError("runcmsgrid.sh already has parallel")
        if "xargs" in line:
          raise IOError("runcmsgrid.sh already has xargs")
      if not sawpowhegcommand:
        raise IOError("runcmsgrid.sh doesn't have this line:\n"+powhegcommand)
    os.chmod("runcmsgrid.sh", os.stat("original_runcmsgrid.sh").st_mode)

    subprocess.check_call(["tar", "cvaf", newfilename] + glob.glob("*"))

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("oldfilename")
  p.add_argument("newfilename")
  args = p.parse_args()
  parallelizepowheg(**args.__dict__)
