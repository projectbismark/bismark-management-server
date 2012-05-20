#!/usr/bin/env python

import calendar
import datetime
import os
import sys
import pprint
import re

import psycopg2

FIXUP_TABLES = ['m_bitrate',
                'm_capacity',
                'm_jitter',
                'm_pktloss',
                'm_shaperate',
                'traceroutes'
                ]

REQ_ENV_VARS = ['BDM_PG_HOST',
                'BDM_PG_USER',
                'BDM_PG_PASSWORD',
                'BDM_PG_MGMT_DBNAME',
                'BDM_PG_DATA_DBNAME',
                ]

# each optional item consists of a tuple (var_name, default_value)
OPT_ENV_VARS = [('BDM_PG_PORT', 5432),
                ('BDMD_DEBUG', 0),
                ]

if __name__ == '__main__':
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

    mconn = psycopg2.connect(
            host=config['BDM_PG_HOST'],
            port=int(config['BDM_PG_PORT']),
            database=config['BDM_PG_MGMT_DBNAME'],
            user=config['BDM_PG_USER'],
            password=config['BDM_PG_PASSWORD'],
            )
    dconn = psycopg2.connect(
            host=config['BDM_PG_HOST'],
            port=int(config['BDM_PG_PORT']),
            database=config['BDM_PG_DATA_DBNAME'],
            user=config['BDM_PG_USER'],
            password=config['BDM_PG_PASSWORD'],
            )

    mcur = mconn.cursor()
    mcur.execute("SELECT DISTINCT ti.ip FROM target_ips as ti ORDER BY ti.ip;")
    dcur = dconn.cursor()
    dcur.execute("CREATE TEMP TABLE mserver_ips ( ip inet PRIMARY KEY );")
    dcur.executemany(
            "INSERT INTO mserver_ips (ip) VALUES (%s);", mcur.fetchall())
    dconn.commit()
    for table in FIXUP_TABLES:
        sys.stdout.write("Adding directions to '%s'..." % table)
        dcur.execute((
                "UPDATE %s SET direction = 'up' "
                "WHERE direction IS NULL "
                "AND dstip IN (SELECT ip FROM mserver_ips);") % table)
        sys.stdout.write(" (%d 'up' row(s), " % dcur.rowcount)
        dcur.execute((
                "UPDATE %s SET direction = 'dw' "
                "WHERE direction IS NULL "
                "AND srcip IN (SELECT ip FROM mserver_ips);") % table)
        sys.stdout.write("%d 'dw' row(s))." % dcur.rowcount)
        dconn.commit()
        print(" Done.")


