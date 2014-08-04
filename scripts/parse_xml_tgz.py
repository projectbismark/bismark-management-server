#!/usr/bin/env python
import os
import sys
import subprocess as sub
import gzip as gz
import time
import pgsql as sql
import bsdtr
import tarfile
import glob
from parser import *

def ignore_file(fname,parsed_files):
  if os.path.basename(fname) in parsed_files:
   return True
  if 'active' not in fname:
    return True
  #if 'OW' not in fname:
  #   return True
  #if 'OW_' in fname:
  #  return True
  return False

def get_parsed_files(parseddb):
  import dbhash as db
  obj = db.open(parseddb,'w')
  return obj

if __name__ == '__main__':
  HOME = os.environ['HOME'] + '/'
  #MEASURE_FILE_DIR = 'var/data/'
  try:
    MEASURE_FILE_DIR = os.environ['MEASURE_FILE_DIR']
  except:
    sys.exit('Required Environment variable MEASURE_FILE_DIR not defined. Terminating')
  LOG_DIR = 'var/log/'
  FILE_LOG = LOG_DIR + 'xml_openwrt_parse_files'
  FILE_PARSED_DB = LOG_DIR + 'parsed_files.db'
  tables = {'measurement':'MEASUREMENTS','traceroute':'traceroutes','hop':'traceroute_hops'}
  filelog = open(HOME+FILE_LOG,'w')
  log = gz.open(HOME+LOG_DIR+'insert.log.gz','ab')
#  files = sub.Popen(['find',MEASURE_FILE_DIR,'-type','f'],stdout=sub.PIPE).communicate()
  files_ow = glob.glob(os.path.join(MEASURE_FILE_DIR,"*/active/OW*/active*"))
  files_dl = glob.glob(os.path.join(MEASURE_FILE_DIR,"*/active/DL*/active*"))
  if files_ow[1] == '':
    sys.exit('Error with glob (OW)')
#  files = files[0].split('\n') not needed anymore
  if files_dl[0] == '':
    sys.exit('Error with glob (DL)')
  fcnt = 0

  files = files_ow + files_dl

  parsed_files = get_parsed_files(HOME+FILE_PARSED_DB)
  for fn in files:
    if ignore_file(fn,parsed_files) == True:
      continue
    f1 = tarfile.open(fn)
    fm1 = f1.getmembers() 
    print 'tarfile:',fn
    for tf in fm1:
      fname = tf.name
      filelog.write("%s\n"%(fname))
      file = f1.extractfile(tf)
      print fname
      parsefile(file,fname,tables,log)
    log.write('Done ' + fn + '\n')
    parsed_files[os.path.basename(fn)] = ''
    parsed_files.sync()
    #move_file(HOME+MEASURE_FILE_DIR+file,HOME+ARCHIVE_DIR)
    fcnt += 1
    if fcnt < -1:
      sys.exit()
  log.close()
  filelog.close()
