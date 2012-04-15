#!/usr/bin/env python

import time
import datetime
import os
import sys
import pprint
import re

import psycopg2

UPDATE_FREQUENCY = datetime.timedelta(days=30)
OLD_DEVICE_THRESHOLD = datetime.timedelta(days=30)
FRESHNESS_THRESHOLD = datetime.timedelta(days=30)
#FRESHNESS_THRESHOLD = datetime.timedelta(days=730)
CLUSTER_PREFERENCES = [30, 20, 10]
MLAB_ONLY = True

REQ_ENV_VARS = ['VAR_DIR',
                'BDM_PG_HOST',
                'BDM_PG_USER',
                'BDM_PG_PASSWORD',
                'BDM_PG_DBNAME',
                'BDM_PG_DATADBNAME',
                ]

# each optional item consists of a tuple (var_name, default_value)
OPT_ENV_VARS = [('BDM_PG_PORT', 5432),
                ('BDMD_TXPG_CONNPOOL', 5),
                ('BDMD_TIME_ERROR', 2),
                ('BDMD_MAX_DELAY', 300),
                ('BDMD_TCP_KEEPIDLE', 10),
                ('BDMD_TCP_KEEPCNT', 2),
                ('BDMD_TCP_KEEPINTVL', 10),
                ('BDMD_DEBUG', 0),
                ]
LOG_SUBDIR = 'log/devices'


# class MserverDatabase
#
# The purpose of this class is to provide a DNS-like interface, along with a
# notion of history (lookups in the past) for measurement server IP addressses
# and fully-qualified domain names (FQDNs). This is necessary because
# measurements sources/destinations are stored as IP addresses, while
# measurement servers are caonically referred to by name (though their IP
# address could theoretically change).
class MserverDatabase(object):
    def __init__(self, dbconn, start_date):
        self.fqdns_by_ip = {}
        self.ips_by_fqdn = {}
        self.id_by_fqdn = {}
        self.fqdn_by_id = {}
        self.fqdn_list = []
        self.fqdns_by_mlab_group = {}  # group eg. atl01, syd02, etc.

        cur = dbconn.cursor()
        cur.execute((
                "SELECT ti.target_id, t.fqdn, ti.ip, ti.date_effective "
                "FROM targets as t, target_ips as ti, ( "
                "    SELECT t.target_id, min(t.min) as min_date "
                "    FROM (( "
                "            SELECT target_id, min(date_effective) "
                "            FROM target_ips "
                "            WHERE date_effective >= %s "
                "            GROUP BY target_id "
                "        ) UNION ( "
                "            SELECT target_id, max(date_effective) "
                "            FROM target_ips "
                "            WHERE date_effective < %s "
                "            GROUP BY target_id "
                "    )) as t "
                "    GROUP BY target_id "
                ") as tmp "
                "WHERE ti.target_id = tmp.target_id "
                "AND t.id = ti.target_id "
                "AND ti.date_effective >= tmp.min_date "
                "ORDER BY target_id, date_effective; "),
                [start_date.isoformat()]*2)
        for row in cur.fetchall():
            self.fqdns_by_ip.setdefault(row[2], []).append({
                    'fqdn': row[1],
                    'date_effective': row[3]})
            self.ips_by_fqdn.setdefault(row[1], []).append({
                    'ip': row[2],
                    'date_effective': row[3]})
            self.id_by_fqdn[row[1]] = row[0]
            self.fqdn_by_id[row[0]] = row[1]
        for k in self.fqdns_by_ip:
            self.fqdns_by_ip[k].sort(
                    key=lambda x: x['date_effective'], reverse=True)
        for k in self.ips_by_fqdn:
            self.ips_by_fqdn[k].sort(
                    key=lambda x: x['date_effective'], reverse=True)

        cur.execute("SELECT fqdn FROM targets")
        self.fqdn_list = [x[0] for x in cur.fetchall()]
        self.fqdn_list.sort()

        for fqdn in self.fqdn_list:
            parts = fqdn.split('.')
            if parts[-3]+'.'+parts[-2] == 'measurement-lab.org':
                self.fqdns_by_mlab_group.setdefault(parts[-4], []).append(fqdn)

    def lookup_ptr(self, ip, date_effective=None):
        if not date_effective:
            date_effective = datetime.datetime.utcnow()
        try:
            fqdn_list = self.fqdns_by_ip[ip]
            for f in fqdn_list:
                if f['date_effective'] < date_effective:
                    return f['fqdn']
            return None
        except KeyError:
            return None

    def lookup_a(self, fqdn, date_effective=None):
        if not date_effective:
            date_effective = datetime.datetime.utcnow()
        try:
            ip_list = self.ips_by_fqdn[fqdn]
            for i in ip_list:
                if i['date_effective'] < date_effective:
                    return i['ip']
            return None
        except KeyError:
            return None

    def lookup_id(self, fqdn):
        return self.id_by_fqdn.get(fqdn, None)


def print_debug_factory(is_debug):
    if is_debug:
        def f(s):
            print(s)
    else:
        def f(s):
            pass
    return f


def print_error(s):
    sys.stderr.write("%s\n" % s)


def print_entry(f):
    def wrapper(*args, **kwargs):
        print_debug(f.func_name)
        return f(*args, **kwargs)
    return wrapper

def find_update_candidates(dbconn):
    cur = dbconn.cursor()
    cur.execute((
            "SELECT t.id, t.min_date "
            "FROM ( "
            "   SELECT dt.device_id as id, min(dt.date_effective) as min_date "
            "   FROM device_targets as dt "
            "   WHERE dt.device_id = dt.device_id "
            "   AND dt.is_permanent = FALSE "
            "   AND dt.is_enabled = TRUE "
            "   GROUP BY dt.device_id "
            "   ) AS t "
            "WHERE t.min_date < %s;"),
            [(datetime.datetime.utcnow() - UPDATE_FREQUENCY).isoformat()])
    outofdate_devices = cur.fetchall()
    cur.execute(
            "SELECT t.did as id, NULL as min_date "
            "FROM ( "
            "   SELECT d.id as did, dt.device_id as dtid "
            "   FROM devices as d"
            "   LEFT OUTER JOIN device_targets as dt ON d.id = dt.device_id "
            "   ) AS t "
            "WHERE dtid IS NULL;")
    unknown_devices = cur.fetchall()
    cur.execute(
            "SELECT devices.id FROM devices WHERE last_seen_ts < %s;",
            [time.mktime((datetime.datetime.utcnow() -
            OLD_DEVICE_THRESHOLD).timetuple())])
    old_devices = [x[0] for x in cur.fetchall()]
    return [x for x in (outofdate_devices + unknown_devices)
            if x[0] not in old_devices]

def select_targets(mgmt_dbconn, data_dbconn, candidates, mserver_db):
    dcur = data_dbconn.cursor()
    device_targets = {}
    for c in candidates:
        dcur.execute((
                "SELECT dstip, eventstamp, "
                "   average, median, minimum, maximum, std "
                "FROM m_mserver_rtt "
                "WHERE deviceid = %s "
                "AND eventstamp > %s "),
                [c[0][2:], (datetime.datetime.utcnow() -
                FRESHNESS_THRESHOLD).isoformat()])

        if dcur.rowcount and dcur.rowcount > 0:
            # device has fresh mserver_rtt data
            ordered_targets = ordered_targets_from_data(
                    dcur.fetchall(), mserver_db)
        elif dcur.rowcount is not None and dcur.rowcount == 0:
            # no rtt data, we need to ping from the servers
            ordered_targets = ordered_targets_from_ping(c[0], mgmt_dbconn)
        else:
            print("ERROR: couldn't get db rowcount")
            ordered_targets = []

        if ordered_targets:
            if MLAB_ONLY:
                device_targets[c[0]] = select_mlab_grouped_targets(
                        ordered_targets, mdb)
    return device_targets

def ordered_targets_from_data(resultset, mserver_db):
    min_latency = {}
    for row in resultset:
        fqdn = mserver_db.lookup_a(row[0], row[1])
        min_latency.setdefault(fqdn, []).append(row[4])
    median_minlatencies = []
    for fqdn in min_latency:
        min_latency[fqdn].sort()
        quick_median = min_latency[fqdn][(len(min_latency[fqdn])+1)/2-1]
        median_minlatencies.append((fqdn, quick_median))
    median_minlatencies.sort(key=lambda x: x[1])
    return [x[0] for x in median_minlatencies]

def ordered_targets_from_ping(deviceid, dbconn):
    return None

def select_mlab_grouped_targets(ordered_targets, mserver_db):
    target_groups = []
    ranked_targets = []
    for fqdn in ordered_targets:
        m = re.search('(\w+)\.measurement-lab.org.$', fqdn)
        if m and m.groups()[0] not in target_groups:
            target_groups.append(m.groups()[0])
            if len(target_groups) == len(CLUSTER_PREFERENCES):
                break
    for i in xrange(len(target_groups)):
        for fqdn in mserver_db.fqdns_by_mlab_group[target_groups[i]]:
            ranked_targets.append((fqdn, CLUSTER_PREFERENCES[i]))
    return ranked_targets

if __name__ == '__main__':
    config = {}
    for evname in REQ_ENV_VARS:
        try:
            config[evname] = os.environ[evname]
        except KeyError:
            print_error(("Environment variable '%s' required and not defined. "
                         "Terminating.") % evname)
            sys.exit(1)
    for (evname, default_val) in OPT_ENV_VARS:
        config[evname] = os.environ.get(evname) or default_val

    print_debug = print_debug_factory(int(config['BDMD_DEBUG']) != 0)
    print_debug(config)

    mconn = psycopg2.connect(
            host=config['BDM_PG_HOST'],
            port=int(config['BDM_PG_PORT']),
            database=config['BDM_PG_DBNAME'],
            user=config['BDM_PG_USER'],
            password=config['BDM_PG_PASSWORD'],
            )
    dconn = psycopg2.connect(
            host=config['BDM_PG_HOST'],
            port=int(config['BDM_PG_PORT']),
            database=config['BDM_PG_DATADBNAME'],
            user=config['BDM_PG_USER'],
            password=config['BDM_PG_PASSWORD'],
            )

    mdb = MserverDatabase(mconn, start_date=(datetime.datetime.utcnow() - FRESHNESS_THRESHOLD))
    update_candidates = find_update_candidates(mconn)
    #updated_device_targets = select_targets(mconn, dconn, update_candidates, mdb)
    #update_targets(conn, updated_device_targets)


# set of tasks
# 2. query for recent m_mserver_rtt for that device
# 3. update based on target data, if available
#       a. if new target, simply pick best m-lab cluster
#       b. if existing target being retargeted, look for best average?
# 4. if successful targets are available, then
#       a. insert new rows into device_targets with current date_effective
#       b. disable old non-permament rows
# BEGIN;
# UPDATE device_targets SET is_enabled = FALSE WHERE is_enabled = TRUE AND
# is_permanent = FALSE;
# INSERT INTO device_targets (device_id, target_id, preference, date_effective,
# is_enabled) VALUES (%s, %s, %s, %s, TRUE)
# COMMIT;
