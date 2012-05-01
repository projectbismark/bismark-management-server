#!/usr/bin/env python

import calendar
import datetime
import os
import sys
import pprint
import socket
import re

import psycopg2

OLD_DEVICE_THRESHOLD = datetime.timedelta(days=7)
FRESHNESS_THRESHOLD = datetime.timedelta(days=30)
#FRESHNESS_THRESHOLD = datetime.timedelta(days=730)

REQ_ENV_VARS = ['VAR_DIR',
                'BDM_PG_HOST',
                'BDM_PG_USER',
                'BDM_PG_PASSWORD',
                'BDM_PG_MGMT_DBNAME',
                'BDM_PG_DATA_DBNAME',
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
            [calendar.timegm((datetime.datetime.utcnow() -
            OLD_DEVICE_THRESHOLD).timetuple())])
    old_devices = [x[0] for x in cur.fetchall()]
    return [x for x in (outofdate_devices + unknown_devices)
            if x[0] not in old_devices]

def filter_devices(mgmt_dbconn, data_dbconn, candidates):
    dcur = data_dbconn.cursor()
    devices_to_ping = []
    candidates = [c for c in candidates if re.match('^OW[0-9A-F]{12}$', c[0])]
    for c in candidates:
        dcur.execute((
                "SELECT dstip, eventstamp, "
                "   average, median, minimum, maximum, std "
                "FROM m_mserver_rtt "
                "WHERE deviceid = %s "
                "AND eventstamp > %s "),
                [c[0][2:], (datetime.datetime.utcnow() -
                FRESHNESS_THRESHOLD).isoformat()])

        if dcur.rowcount is not None and dcur.rowcount == 0:
            # no rtt data, we need to ping from the servers
            devices_to_ping.append(c[0])

def ping_devices(mgmt_dbconn, data_dbconn, devices, mserver_db):
    dcur = data_dbconn.cursor()
    dcur.execute((
            # TODO: need to account for direction here!!
            "SELECT t1.deviceid, t1.id, t1.eventstamp "
            "FROM traceroutes as t1 "
            "WHERE t1.eventstamp >= ( "
            "   SELECT max(t2.eventstamp) "
            "   FROM traceroutes as t2 "
            "   WHERE t2.deviceid = t1.deviceid "
            "   ) "
            "AND t1.eventstamp > %s "
            "ORDER BY t1.eventstamp;"),
            [(datetime.datetime.utcnow() - FRESHNESS_THRESHOLD).isoformat()])
    rows = dcur.fetchall()
    traceroute_ids = dict([r[0:20 for r in rows])
    for d in devices:


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

    mdb = MserverDatabase(
            mconn,
            start_date=(datetime.datetime.utcnow() - FRESHNESS_THRESHOLD))
    update_candidates = find_update_candidates(mconn)
    pprint.pprint(update_candidates)
    #dcur = dconn.cursor()
    #dcur.execute('select distinct deviceid from m_mserver_rtt;')
    #update_candidates = [('OW'+''.join(x[0].split(':')).upper(),) for x in dcur.fetchall()]
    #pprint.pprint(update_candidates)
    ping_candidates = filter_devices(mconn, dconn, update_candidates)
    pprint.pprint(ping_candidates)
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

def mserver_ping(ip, mserver_hostname):
    ping_out = {}
    output = netcat(mserver_hostname, 1101, 'ping %s -q\n' % ip)
    try:
        groups = re.match(
                '(\d+) packets transmitted, (\d+) received',
                output[2]).groups()
        ping_out['count_sent'] = int(groups[0])
        ping_out['count_recv'] = int(groups[1])
    except (AttributeError, IndexError):
        pass
    try:
        groups = re.match(
                'rtt min/avg/max/mdev = '
                '((?:\d+(?:\.\d+)?/){3}(?:\d+(?:\.\d+)?))',
                output[3]).groups()[0].split('/')
        ping_out['rtt_min']    = float(groups[0])
        ping_out['rtt_avg']    = float(groups[1])
        ping_out['rtt_max']    = float(groups[2])
        ping_out['rtt_stddev'] = float(groups[3])
    except (AttributeError, IndexError):
        pass
    return ping_out

# based on
# http://stackoverflow.com/questions/1908878/netcat-implementation-in-python
def netcat(hostname, port, content):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    output = []
    try:
        s.connect((hostname, port))
        s.sendall(content)
        s.settimeout(20)
        while 1:
            data = s.recv(1024)
            if data == "":
                break
            output.extend(data.strip().split('\n'))
        s.close()
    except socket.timeout:
        output = []
    return [x for x in output if x]
