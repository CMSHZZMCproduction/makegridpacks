# makegridpacks

This script is meant to run many of the steps of Monte Carlo requests on CMS.
It takes care of making the gridpack, creating the request on McM, getting the filter efficiency
and size and time per event, running the validation and finding the best number of CPUs to use,
and defining the request.

## Cookie

The ONLY way to access McM is to initialize a cookie.
However, you can't initialize one if you've `cmsenv`ed, and you have to `cmsenv` to run this script.
So what you have to do is log in, create the cookie by running
```
source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh
```
and then `cmsenv`.  The cookie is only valid on the same lxplus where you created it,
but it's valid across multiple sessions on that lxplus.  If you've already `cmsenv`ed,
you can still get the cookie by doing
```
ssh $(hostname) source /afs/cern.ch/cms/PPD/PdmV/tools/McM/getCookie.sh
```
because the ssh session doesn't know about your `cmsenv`.

## Setup

You want to do this in a public location on lxplus, probably `work/public`, so that
you have space to store gridpacks and so that the HIG contacts can copy them for you

```
cmsrel CMSSW_9_3_0
cd CMSSW_9_3_0/src
cmsenv
git clone git@github.com:cms-sw/genproductions  #or your fork
git clone git@github.com:CMSHZZMCproduction/makegridpacks
```

## Running

To tell the script which samples you want to produce, you have to edit the classes
in `helperstuff/`.  Once you've done that, the basic procedure is to run `./makegridpacks.py`
multiple times.  Each time, the script will loop over all the samples, check what each one
is up to, and do the next step if possible.  These are some of the steps in more detail.

### Creating the gridpack

This usually works by running a script provided in the genproductions repository.
If that script takes a long time, `makegridpacks.py` will check if it's running on a queue,
and if not submit a job to a queue to run that script.

Sometimes (POWHEG multicore and JHUGen VBF offshell) the script takes several steps, and in
that case makegridpacks.py will check what it's up to and submit the next step once the first one
is done.

It's possible to apply patches to the gridpack after it's made.  Changing anything that affects the
core process is a terrible idea because it would mean that the grid saved in the gridpack is wrong,
but you can change PDF reweighting or JHUGen decay for instance.

The gridpackbysomeoneelse.py class is meant for gridpacks that someone else made.  In that case
"making the gridpack" just means copying it and possibly applying a patch.

The gridpack gets put in the `gridpacks/` folder.

### Copying to eos

Subgroup contacts don't have permissions to do this, so you have to ask the HIG contacts.
The easiest way is to paste this line into skype
```
cp -r /.../CMSSW_9_3_0/src/makegridpacks/gridpacks/* /eos/cms/store/group/phys_generator/cvmfs/gridpacks/
```
and of course describe what the gridpacks are for.

Once they copy it, if you run makegridpacks.py again it will say that it's been copied to eos.
Eventually, the automatic system will copy the gridpack to cvmfs, where it's supposed to live.
At that point, when you run `makegridpacks.py` again, it will compare the one in cvmfs to the local
one, and if they're identical and nothing got corrupted in the copy, it will delete the local one.
You can use `helperstuff/cleanupgridpacks.py` to remove the empty folders.  (Just don't do this
while you have jobs running to create gridpacks, or it might delete a folder as the script was
about to put something in it.)

### Creating the request on McM

*For this and all subsequent steps, you need a cookie.*

The next step is to send the request to McM.  `makegridpacks.py` will do that for you
after making some checks on the tarball.

The checks are defined in the `cardsurl` function of each sample class.
One common check is that there's no core dump in the tarball, which
would indicate that the job to create the grid failed.  We also check that the cards
in genproductions are identical to the ones in the tarball, which works differently
for every generator because the cards are stored in different places in the
tarball's directory structure.

Do *not* bypass these cross checks!  If anything, you should add more of them.
If there's something that really doesn't matter, like changed comments
in the input cards or stuff that gets modified automatically by the
gridpack creation script, you can change the function to allow that,
but don't just make an exception because you don't understand something.
It will waste months of work.

The prepid of the request is stored in McMsampleproperties.json.  More information
about the request will be stored there later.

### Filter efficiency

We have to find the filter efficiency so that McM knows how many events
to request in order to get the right number in the final output.

If a request doesn't have a filter, this step just sets the efficiency to 1.
If it does have a filter, it will submit 100 jobs to LSF and record the number
of passing events and the total number of events for each one.  Once they're all
done, it calculates the filter efficiency and its error and saves that
in McMsampleproperties.json.

For pythia filters, there's no choice but to run the full chain.  For JHUGen level
filters, there's a special implementation to run just the gridpack and get the
filter efficiency from there.

### Size and time per event

We have to run a test job to estimate the disk space and time needed for each event.
If there's a pythia filter, this was already done at the same time as the filter
efficinecy.  Otherwise we do it now.  This is again saved in McMsampleproperties.json.

### Updating on McM

Any time parameters like the filter efficiency or time/event change, we have to
update the request on McM.  To save time, the other parameters are not recomputed
automatically.  If you change the fragment or nevents, you should run
```
./makegridpacks.py --setneedsupdate
```
to turn on the `needsupdate` flag for requests that have changed.

### request_fragment_check.py

This is a script from the GEN conveners to check the fragment.
Typically most of the errors and warnings should be covered by the cross
checks in makegridpacks.py.  If there are any unhandled errors or warnings,
makegridpacks.py will print them and not take the request any further.

One common one is a large time/event.  You can bypass this if it's not too big
([example](https://github.com/CMSHZZMCproduction/makegridpacks/blob/628f4a3dd768d8f3c4b604a66b2e4424f5cdba95/helperstuff/powhegjhugenmassscanmcsample.py#L307)).
Warnings about "check the POWHEG nfinal" and stuff like that is automatically handled
by setting nfinalparticles or similar functions, to show that you've thought about it.

Don't bypass these without thinking!

### validation

If `request_fragment_check.py` passes, the script triggers McM to start the validation.

If you don't want to run validation for a particular sample because it keeps failing,
set dovalidation to return False for it.

### nthreads

If the validation fails, the most common reason is CPU efficiency.  Unfortunately the reason
for failure is not saved in the McM API, so we just assume it's that and reduce the threads
by a factor of 2, as recommended by PdMV.  Hopefully the API will be updated soon so
we don't have to do this...

Changing nthreads also deletes the timeperevent, so at this point we go back a few steps.

### define

If the validation passes, the script defines the request

### ticket

(will add more information here later)

## Sample configuration

The sample classes are designed to use python's multiple inheritance.

The main class is MCSampleBase, and everything inherits from that.  It
contains functions to do everything described above, and abstract methods
and properties to be filled by child classes.  There are also intermediate
classes, like POWHEGMCSample, that fill some of these abstract methods
and also define their own abstract methods.

The final class has to define all the abstract stuff.  Once you do that,
import it in the `allsamples` function in `helperstuff/__init__.py`.
Then, when you try to run `makegridpacks.py`, it will tell you if there are
any abstract methods or properties you missed, and if not it will loop
over the samples defined by the class

(will fill this more later)
