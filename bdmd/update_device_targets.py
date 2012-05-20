#!/usr/bin/env python

import datetime
import os
import sys
import re

import psycopg2

UPDATE_FREQUENCY = datetime.timedelta(days=30)
OLD_DEVICE_THRESHOLD = datetime.timedelta(days=30)
FRESHNESS_THRESHOLD = datetime.timedelta(days=30)
MLAB_GROUP_PREFERENCES = [30, 20, 10]
MLAB_ONLY = True
GLOBAL_UTCNOW = datetime.datetime.utcnow()

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
            date_effective = GLOBAL_UTCNOW
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
            date_effective = GLOBAL_UTCNOW
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


def print_error(s):
    sys.stderr.write("%s\n" % s)

def find_update_candidates(mgmt_dbconn):
    cur = mgmt_dbconn.cursor()
    cur.execute((
            "SELECT t.id, t.min_date "
            "FROM ( "
            "   SELECT dt.device_id as id, min(dt.date_effective) as min_date "
            "   FROM device_targets as dt "
            "   WHERE dt.is_permanent = FALSE "
            "   AND dt.is_enabled = TRUE "
            "   GROUP BY dt.device_id "
            "   ) AS t, devices AS d "
            "WHERE t.min_date < %s "
            "AND t.id = d.id "
            "AND d.date_last_seen >= %s;"),
            [(GLOBAL_UTCNOW - UPDATE_FREQUENCY),
             (GLOBAL_UTCNOW - OLD_DEVICE_THRESHOLD)])
    outofdate_devices = cur.fetchall()
    cur.execute((
            "SELECT t.did as id, NULL as min_date "
            "FROM ( "
            "   SELECT d.id as did, dt.device_id as dtid "
            "   FROM devices as d"
            "   LEFT OUTER JOIN device_targets as dt ON d.id = dt.device_id "
            "   WHERE d.date_last_seen >= %s "
            "   ) AS t "
            "WHERE dtid IS NULL;"),
            [(GLOBAL_UTCNOW - OLD_DEVICE_THRESHOLD)])
    unknown_devices = cur.fetchall()
    devices = outofdate_devices + unknown_devices
    return [d for d in devices if re.match('^OW[0-9A-F]{12}$', d[0], re.I)]

def select_device_targets(data_dbconn, device_id, mserver_db):
    dcur = data_dbconn.cursor()
    device_targets = []
    rtt_data = None
    dcur.execute((
            "SELECT dstip, eventstamp, "
            "   average, median, minimum, maximum, std "
            "FROM m_mserver_rtt "
            "WHERE deviceid = %s "
            "AND eventstamp > %s "),
            [device_id[2:], (GLOBAL_UTCNOW - FRESHNESS_THRESHOLD)])

    if dcur.rowcount and dcur.rowcount > 0:
        # device has fresh mserver_rtt data
        rtt_data = dcur.fetchall()
    elif dcur.rowcount is not None and dcur.rowcount == 0:
        # no rtt data, we need to use mserver -> device ping data
        dcur.execute((
                "SELECT dstip, eventstamp, "
                "   average, median, minimum, maximum, std "
                "FROM m_mserver_rping "
                "WHERE deviceid = %s "
                "AND eventstamp > %s "),
                [device_id[2:], (GLOBAL_UTCNOW - FRESHNESS_THRESHOLD)])
        if dcur.rowcount and dcur.rowcount > 0:
            rtt_data = dcur.fetchall()
    else:
        print_error("ERROR: couldn't get db rowcount")

    if rtt_data:
        ordered_targets = select_targets_by_rtt(rtt_data, mserver_db)
        if ordered_targets:
            if MLAB_ONLY:
                device_targets = select_mlab_targets_by_group(
                        ordered_targets, mserver_db)
    else:
        print_error("ERROR: device '%s' has no RTT data." % device_id)

    return device_targets

def select_targets_by_rtt(rtt_resultset, mserver_db):
    min_latency = {}
    for row in rtt_resultset:
        fqdn = mserver_db.lookup_ptr(row[0], row[1])
        min_latency.setdefault(fqdn, []).append(row[4])
    median_minlatencies = []
    for fqdn in min_latency:
        min_latency[fqdn].sort()
        quick_median = min_latency[fqdn][(len(min_latency[fqdn])+1)/2-1]
        median_minlatencies.append((fqdn, quick_median))
    median_minlatencies.sort(key=lambda x: x[1])
    return median_minlatencies

def select_mlab_targets_by_group(ordered_targets, mserver_db):
    target_groups = []
    ranked_targets = []
    for fqdn, _ in ordered_targets:
        m = re.search('(\w+)\.measurement-lab.org.$', fqdn)
        try:
            mlab_group = m.groups()[0]
        except AttributeError:
            mlab_group = None
        if mlab_group and mlab_group not in target_groups:
            target_groups.append(mlab_group)
            if len(target_groups) == len(MLAB_GROUP_PREFERENCES):
                break
    for i in xrange(len(target_groups)):
        for fqdn in mserver_db.fqdns_by_mlab_group[target_groups[i]]:
            ranked_targets.append((fqdn, MLAB_GROUP_PREFERENCES[i]))
    return ranked_targets

def apply_device_targets(mgmt_dbconn, device_id, ranked_targets, mserver_db):
    if ranked_targets:
        cur = mgmt_dbconn.cursor()
        cur.execute((
                "UPDATE device_targets "
                "SET is_enabled = FALSE "
                "WHERE is_enabled = TRUE "
                "AND is_permanent = FALSE "
                "AND device_id = %s;"),
                [device_id])
        target_tuples = [
                (device_id, mserver_db.lookup_id(fqdn), pref, GLOBAL_UTCNOW)
                for (fqdn, pref) in ranked_targets]
        cur.executemany((
                "INSERT INTO device_targets "
                "(device_id, target_id, preference, date_effective, "
                "is_enabled, is_permanent) "
                "VALUES (%s, %s, %s, %s, TRUE, FALSE);"),
                target_tuples)
        mgmt_dbconn.commit()

def main(config):
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
            start_date=(GLOBAL_UTCNOW - FRESHNESS_THRESHOLD))
    update_candidates = find_update_candidates(mconn)
    for device_id, _ in update_candidates:
        device_targets = select_device_targets(dconn, device_id, mdb)
        #apply_device_targets(mconn, device_id, device_targets, mdb)


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

    main(config)
