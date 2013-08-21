#!/usr/bin/python 

from gzip import GzipFile as gz
import psycopg2 as pgsql
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
          database=config['BDM_PG_DATA_DBNAME'],
          host=config['BDM_PG_HOST'],
          user=config['BDM_PG_USER'],
          password=config['BDM_PG_PASSWORD'])
    cursor = conn.cursor() 
  except:
    print "Could not connect to sql server"
    sys.exit()
  return cursor

def run_insert_cmd(cmds,conn=None,prnt=0):
  if conn == None:
    conn = sqlconn()
  savepointcmd = 'savepoint sp;'
  #print cmds
  conn.execute('begin')
  conn.execute(savepointcmd)
  print 'begin'
  for ctup in cmds:
    cmd = ctup[0]
    cvals = ctup[1]
    try:
      conn.execute(cmd,cvals)
      conn.execute(savepointcmd)
      if prnt == 1:
        print cmd,cvals
    except:
      print "Couldn't run ",cmd,cvals
      conn.execute('rollback to savepoint sp')
    #cursor.fetchall()
  print 'end'
  conn.execute('commit')
  return 1 

def run_data_cmd(cmd,cvals,conn=None,prnt=0):
  if conn == None:
    conn = sqlconn()
  res = ''
  if prnt == 1:
    print cmd,cvals
  try:
    conn.execute(cmd,cvals)
  except:
    #print conn.error
    print "Couldn't run ", cmd,cvals
    return 0 
  result = conn.fetchall()
  return result 
