#!/usr/bin/env python
import os
import sys
import subprocess as sub
import gzip as gz
import time
import pgsql as sql
import bsdtr
from parser import *

def move_file(file,dir):
  cmd = ['gzip',file]
  sub.Popen(cmd).communicate()
  zfile = file + '.gz'
  cmd = ['mv',zfile,dir]
  sub.Popen(cmd).communicate()

def ignore_file(file):
  if '.xml' not in file:
    return True
  if 'OW' not in file:
    return True
  if 'OW_' in file:
    return True
  return False

if __name__ == '__main__':
  HOME = os.environ['HOME'] + '/'
  MEASURE_FILE_DIR = 'var/data/'
  LOG_DIR = 'var/log/'
  ARCHIVE_DIR = 'var/archive/openwrt'
  FILE_LOG = LOG_DIR + 'xml_openwrt_parse_files'
  tables = {'measurement':'MEASUREMENTS','traceroute':'traceroutes','hop':'traceroute_hops'}

  filelog = open(HOME+FILE_LOG,'w')
  log = gz.open(HOME+LOG_DIR+'insert.log.gz','ab')
  files = os.listdir(HOME+MEASURE_FILE_DIR)
  fcnt = 0
  for file in files:
    if ignore_file(file) == True:
      continue
    else:
      filelog.write("%s\n"%(file))
    print file
    fcnt += 1
    fp = open(HOME+MEASURE_FILE_DIR+file)
    parsefile(fp,file,tables,log)
    log.write('Done ' + file + '\n')
    move_file(HOME+MEASURE_FILE_DIR+file,HOME+ARCHIVE_DIR)
    if fcnt < -1:
      sys.exit()
  log.close()
  filelog.close()

