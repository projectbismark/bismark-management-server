#!/usr/bin/env python
import os
import sys
import subprocess as sub
import gzip as gz
import time
import pgsql as sql
import bsdtr

conn = sql.sqlconn()
traceroutearr = {}
bsdtr.init()

def get_fields(line):
  skey = ''
  if '/>' in line:
    skey = '/>'
  else:
    skey = '>'

  try:
    line = line.split('<')[1]
  except:
    return None
  line = line.split(skey)[0]
  line = line.replace("'","")
  line = line.replace('"','')
  val = line.split()
  return val

def get_measurement_params(fids,vals,arr):
  #print arr
  for fid in arr:
    fids.append(fid)
    vals.append(arr[fid])
  return fids,vals

def modify_fid(fid,table):
  if fid == 'timestamp':
    return 'eventstamp'
  if table == 'traceroute_hops':
    if fid == 'id':
      return 'hop'
    if fid == 'tid':
      return 'id'

  if table == 'MEASUREMENTS':
    if fid == 'avg':
      return 'average'
    if fid == 'min':
      return 'minimum'
    if fid == 'max':
      return 'maximum'
    if fid == 'med':
      return 'median'
  return fid

def modify_val(fid,val):
  nstr = '%s'
  nval = val
  if val == '':
    nval = 'NULL'
  #nval = "'" + val + "'"
  if fid  == 'timestamp':
    nstr = 'to_timestamp(%s)'#%(val)
  if fid  == 'deviceid':
    nval = '%s'%(val[-12:])
 #if fid in ['deviceid','param','tool']:
  #  nval = '"' + val + '"'
  #else:
    #if fid in ['srcip','dstip','ip']:
    #  nval = 'INET_ATON("' + val + '")'
  return nstr,nval

def get_tool_info(tool):
  cmd = 'select id from tools where tool = "%s"'%(tool)
  #res = sql.run_sql_cmd(cmd)
  #return int(res[0][0])
  #return 'asdasd'
  return tool
  
def form_insert_cmd(table,fids,vals):
  if table.lower() == 'measurements':
    tabfid = fids.index('param')
    ntab = '%s_%s'%(table.lower()[0],vals[tabfid].lower())
    fids.pop(tabfid)
    vals.pop(tabfid)
    toolind = fids.index('tool')
    tool = vals[toolind]
    toolid = get_tool_info(tool)
    fids[toolind] = 'toolid'
    vals[toolind] = toolid
  else:
    ntab = table
  cmd = 'INSERT into ' + ntab + '('
  if 'traceroute' not in table:
    cmd += 'exitstatus,'
  for fid in fids:
    fid = modify_fid(fid,table)
    cmd += fid + ","
  cmd = cmd[0:len(cmd)-1]
  cmd += ') SELECT '
  if 'traceroute' not in table:
    cmd += '0,'
  cvals = []
  for val in vals:
    ind = vals.index(val)
    nstr,nval = modify_val(fids[ind],val)
    cmd += nstr + ","
    cvals.append(nval)
  cmd = cmd[0:len(cmd)-1]
  #print cmd
  return cmd,cvals

def write_block_v1_0(data,tables,log,fname):
  if 'info' not in data:
    log.write('Error: No info field in %s\n'%(fname))
    return
  flag = 0
  for tab in tables:
    if tab in data:
      flag = 1
      break
  if flag == 0:
    log.write('Error: No known fields in %s\n'%(fname))
    return

  #print data
  #postcmds = ['begin']
  postcmds = []
  global traceroutearr
  for tab in tables:
    if tab in data:
      numrec = len(data[tab])
      for i in range(0,numrec):
        table = tables[tab]
        fids = []
        vals = []
        if tab != 'hop':
          fids,vals = get_measurement_params(fids,vals,data['info'][0])
          fids,vals = get_measurement_params(fids,vals,data[tab][i])
          cmd,cvals = form_insert_cmd(table,fids,vals)
        else:
          ttid = data[tab][i]['ttid']
          data[tab][i].pop('ttid')
          did = data['info'][0]['deviceid'][-12:]
          ts = data['traceroute'][ttid]['timestamp']
          srcip = data['traceroute'][ttid]['srcip']
          dstip = data['traceroute'][ttid]['dstip']
          try: 
            toolid = data['traceroute'][ttid]['tool']
          except:
            toolid = 'traceroute'
          tup = (did,ts,srcip,dstip,toolid)
          #idtuple = {"tid":''}
          #print tab
          #fids,vals = get_measurement_params(fids,vals,idtuple)
          fids,vals = get_measurement_params(fids,vals,data[tab][i])
          hopid = vals[fids.index('id')]
          hopip = vals[fids.index('ip')]
          hoprtt = vals[fids.index('rtt')]
          hopval = (hopid,hopip,hoprtt)
          try:
            traceroutearr[tup].append(hopval)
          except:
            traceroutearr[tup] = [hopval]
        #print cmd,table
        #if tab == 'traceroute':
        #  cmd = "%s returning encode(id,'escape')"%(cmd)
        #  #res = sql.run_data_cmd(cmd,cvals,conn=conn,prnt=1) # to get return value
        #  did = data['info'][0]['deviceid'][-12:]
        #  ts = vals[fids.index('timestamp')]
        #  srcip = vals[fids.index('srcip')]
        #  dstip = vals[fids.index('dstip')]
        #  if 'tool' in fids:
        #    toolid = vals[fids.index('tool')]
        #  else:
        #    toolid = 'traceroute'
        #  tup = (did,ts,srcip,dstip,toolid)
        #  traceroutearr[tup] = []
        if tab != 'hop' and tab != 'traceroute':
          postcmds.append([cmd,cvals])
  if len(postcmds) > 1:
    #postcmds.append('commit')
    sql.run_insert_cmd(postcmds,conn=conn)
  if len(traceroutearr) > 0:
    bsdtr.write(traceroutearr)
    traceroutearr = {}
    

def parse_block_v1_0(block,version,tables,log):
  data = {}
  for line in block:
    fields = get_fields(line)
    if fields == None:
      continue
    head = fields[0]
    if '/' in head:
      continue
    if head not in data:
      data[head] = []

    tuple = {}
    if head == 'hop':
      tuple['ttid'] = len(data['traceroute']) -1
    for field in fields[1:]:
      field = field.split("=")
      name = field[0]
      val = field[1]
      tuple[name] = val
      #val = field[1].split('"')[1]
      #print name,":", val, ",",
    #print ''
    data[head].append(tuple)
  return data

def parse_block_v1_1(block,version,tables,log):
  data = parse_block_v1_0(block,version,tables,log)
  return data

def parse_block(block,version,tables,log,fname):
  if version == '1.0':
    data = parse_block_v1_0(block,version,tables,log)
    write_block_v1_0(data,tables,log,fname)
  if version == '1.2' or version == '1.3':
    data = parse_block_v1_1(block,version,tables,log)
    did = data['info'][0]['deviceid']
    write_block_v1_0(data,tables,log,fname)
  return True

def log_bad_block(log,block,fname):
  log.write('Bad block in %s\n'%(fname))
  for line in block:
    log.write('%s\n'%(line))

def parsefile(file,fname,tables,log):
  start_block = '<measurements'
  end_block = '</measurements'
  #fp = open(file)
  fp = file.readlines()
  state = 0
  block = []
  version = 0
  for line in fp:
    if state == 0:
      if  start_block in line:
        state = 1
        val = get_fields(line)
        version = val[1].split("=")[1]#.split('"')[1]
        print version
      continue

    if state == 1:
      if end_block in line:
        stat = parse_block(block,version,tables,log,fname)
        if stat == False:
          log_bad_block(log,block,fname)
        state = 0
        block = []
        continue
      block.append(line)

