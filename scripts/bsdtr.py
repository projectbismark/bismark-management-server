#!/usr/bin/python 

from gzip import GzipFile as gz
import dbhash as db
import sys
import datetime
import pickle
import os

REQ_ENV_VARS = ['BDM_BSD_FILE']

config = {}
def init():
  for evname in REQ_ENV_VARS:
    try:
        config[evname] = os.environ[evname]
    except KeyError:
      print(("Environment variable '%s' required and not defined. "
                "Terminating.") % evname)
      sys.exit(1)


def get_new_obj(dbpart):
  traceroutetable = config['BDM_BSD_FILE']
  try:
    obj = db.open('%s-%s.db'%(traceroutetable,dbpart),'w')
    print 'opening %s-%s.db'%(traceroutetable,dbpart)
    return obj
  except:
    return None
    
dbobjs = {}
def write(inarr):
  cobj = {}
  for tup in inarr:
    ptup = pickle.dumps(tup)
    pval = pickle.dumps(inarr[tup])
    ts = int(tup[1])
    dbpart = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m')
    try:
      dbobjs[dbpart][ptup] = pval
    except:
      nobj = get_new_obj(dbpart)
      if nobj == None:
        sys.exit("Error opening BSD DB file")
      dbobjs[dbpart] = nobj
      dbobjs[dbpart][ptup] = pval
    print tup,inarr[tup]
    cobj[dbpart] = 0
  for part in cobj:
    dbobjs[part].sync()

