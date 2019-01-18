#!/usr/bin/env python

from mcsamplebase import MCSampleBase
from powhegmcsample import POWHEGMCSample

class GridpackOnly(MCSampleBase):
  @property
  def campaign(self): assert False
  @property
  def datasetname(self): assert False
  @property
  def defaulttimeperevent(self): assert False
  @property
  def hasfilter(self): assert False
  @property
  def nevents(self): assert False
  @property
  def tags(self): assert False
  @property
  def xsec(self): assert False

  @property
  def makerequest(self):
    return False

class POWHEGGridpackOnly(POWHEGMCSample, GridpackOnly):
  @property
  def nfinalparticles(self): assert False
