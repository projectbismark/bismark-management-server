#!/usr/bin/python 

from gzip import GzipFile as gz
import pg as pgsql
import sys
import traceback
import os
import random as rnd
import socket, struct
import numpy as np

REQ_ENV_VARS = ['BDM_PG_HOST',
                'BDM_PG_USER',
                'BDM_PG_PASSWORD',
                'BDM_PG_DATA_DBNAME',
                ]

OPT_ENV_VARS = [('BDM_PG_PORT', 5432),
                ]

def sqlconn():
  config = {}
  for evname in REQ_ENV_VARS:
    try:
        config[evname] = os.environ[evname]
    except KeyError:
      print(("Environment variable '%s' required and not defined. "
                "Terminating.") % evname)
      sys.exit(1)
  for (evname, default_val) in OPT_ENV_VARS:
    config[evname] = os.environ.get(evname) or default_val

  try:
    conn = pgsql.connect(
          dbname=config['BDM_PG_DATA_DBNAME'],
          host=config['BDM_PG_HOST'],
          user=config['BDM_PG_USER'],
          passwd=config['BDM_PG_PASSWORD'])
  #cursor = conn.cursor() 
  except:
    print "Could not connect to sql server"
    sys.exit()
  return conn

def run_insert_cmd(cmd,conn=None,prnt=0):
  if conn == None:
    conn = sqlconn()
  #print cmd
  try:
    conn.query(cmd)
  except:
    print "Couldn't run %s\n"%(cmd)
    return 0 
  #cursor.fetchall()
  return 1 

def run_data_cmd(cmd,conn=None,prnt=0):
  if conn == None:
    conn = sqlconn()
  res = ''
  if prnt == 1:
    print cmd
  try:
    res = conn.query(cmd)
  except:
    #print "Couldn't run %s\n"%(cmd)
    return 0 
  result = res.getresult()
  return result 
