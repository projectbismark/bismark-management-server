#!/usr/bin/env python

import datetime
import os
import sys
import argparse
import json
import pprint
import csv
import tarfile
import subprocess
import psycopg2

#import psycopg2

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

class ExportTask(object):
    """This class represents an export task: a specification of a set of
    BISmark devices, measurement servers, and measurement data tables. The
    union of these three sets (i.e. data from tables A and B, measured by
    routers X and Y using servers P and Q) defines the data that will be
    exported to the specified destination."""

    def __init__(self, json_task):
        self.valid = True
        # required properties
        try:
            self.unique_id = json_task.get('unique_id')#, None)
            self.servers = json_task.get('servers')#, [])
            self.devices = json_task.get('devices')#, [])
            self.tables  = json_task.get('tables')#, [])
            self.scp_destination = json_task.get('scp_destination')#, None)
            self.start_date = datetime.datetime.strptime(
                    json_task.get('start_date'), '%Y-%m-%d').date()
        except (KeyError, ValueError):
            self.valid = False

        # optional properties (with defaults)
        self.include_deviceid = json_task.get('include_deviceid', False)

        # date of last successful dump to local storage
        self.last_local_export = None
        # date of last successful copy to remote storage
        self.last_remote_export = None

        if self.valid:
            self.valid = self.is_valid()

    def load_state(self, json_state):
        if not json_state:
            return

        if 'last_local_export' in json_state:
            self.last_local_export = datetime.datetime.strptime(
                    json_state['last_local_export'], '%Y-%m-%d').date()
        else:
            self.last_local_export = None

        if 'last_remote_export' in json_state:
            self.last_remote_export = datetime.datetime.strptime(
                    json_state['last_remote_export'], '%Y-%m-%d').date()
        else:
            self.last_remote_export = None

    def dump_state(self, json_state):
        json_state['last_local_export'] = self.last_local_export
        json_state['last_remote_export'] = self.last_remote_export

    def scp_to_destination(self, path, dry_run=False):
        try:
            u = self.scp_destination['user']
            h = self.scp_destination['host']
            r = self.scp_destination['remote_root']
            k = self.scp_destination['keyfile']
            if dry_run:
                print("calling %s" % ' '.join(
                      ['/usr/bin/scp', '-i', k, path,  '%s@%s:%s' % (u,h,r)]))
            else:
                subprocess.check_call(
                        ['/usr/bin/scp', '-i', k, path,  '%s@%s:%s' % (u,h,r)])
        except CalledProcessError:
            print("scp problem...")
        except KeyError as ke:
            print(ke)

    def is_valid(self):
        valid = True
        if not self.unique_id:
            valid = False
        if not filter(self.__class__._valid_wildcard, self.servers):
            valid = False
        if not filter(self.__class__._valid_wildcard, self.devices):
            valid = False
        if not filter(self.__class__._valid_wildcard, self.tables):
            valid = False
        if not self.scp_destination:
            valid = False
        return valid

    def next_local_export_date(self):
        if not self.last_local_export:
            return self.start_date
        else:
            return max(
                    self.start_date,
                    self.last_local_export + datetime.timedelta(days=1))

    def next_remote_export_date(self):
        if not self.last_remote_export:
            return self.start_date
        else:
            return max(
                    self.start_date,
                    self.last_remote_export + datetime.timedelta(days=1))

    @staticmethod
    def _valid_wildcard(s):
        """
        Return a bool whether the provided string matches the current wildcard
        protocol: only one wildcard is allowed per string, at the beginning or
        the end of the string only.
        """
        if s[-1] == '*':
            s = s[:-1]
        elif s[0] == '*':
            s = s[1:]
        return s.find('*') == -1

    @staticmethod
    def _compare_wildcard(wc, s):
        wc = wc.lower()
        s = s.lower()
        if wc[-1] == '*':
            return s[:len(wc[:-1])] == wc[:-1]
        elif wc[0] == '*':
            return s[-len(wc[1:]):] == wc[1:]
        return wc == s

    def filter_servers(self, server_list):
        return self._filter_list(server_list, self.servers)

    def filter_devices(self, device_list):
        return self._filter_list(device_list, self.devices)

    def filter_tables(self, table_list):
        return self._filter_list(table_list, self.tables)

    def _filter_list(self, concrete_list, wc_collection):
        out = []
        for w in wc_collection:
            out.extend(filter(lambda x: self.__class__._compare_wildcard(w, x),
                              concrete_list))
        return out

    def device_required(self, device):
        return self._in_wc_collection(device, self.devices)

    def server_required(self, server):
        return self._in_wc_collection(server, self.servers)

    def table_required(self, table):
        return self._in_wc_collection(table, self.tables)

    def _in_wc_collection(self, s, wc_collection):
        return any(map(lambda x: self.__class__._compare_wildcard(x, s),
                       wc_collection))

class ExportProcessor(object):

    ALL_TABLES = [
            'm_aggl3bitrate',
            'm_bitrate',
            'm_capacity',
            'm_dnsdelay',
            'm_dnsdelayc',
            'm_dnsdelaync',
            'm_dnsfail',
            'm_dnsfailc',
            'm_dnsfailnc',
            'm_jitter',
            'm_lmrtt',
            'm_pktloss',
            'm_rtt',
            'm_shaperate',
            'm_ulrttdw',
            'm_ulrttup'
            ]

    FRIENDLY_NAMES = {
        'm_aggl3bitrate' : 'aggl3bitrate',
        'm_bitrate'      : 'bitrate',
        'm_capacity'     : 'capacity',
        'm_dnsdelay'     : 'dnsdelay',
        'm_dnsdelayc'    : 'dnsdelayc',
        'm_dnsdelaync'   : 'dnsdelaync',
        'm_dnsfail'      : 'dnsfail',
        'm_dnsfailc'     : 'dnsfailc',
        'm_dnsfailnc'    : 'dnsfailnc',
        'm_jitter'       : 'jitter',
        'm_lmrtt'        : 'lmrtt',
        'm_pktloss'      : 'pktloss',
        'm_rtt'          : 'rtt',
        'm_shaperate'    : 'shaperate',
        'm_ulrttdw'      : 'ulrttdw',
        'm_ulrttup'      : 'ulrttup'
        }

    def __init__(self, state_path, settings_path, dry_run=False):
        self.settings = {}
        self.state = {}
        self.tasks = []
        self.dry_run = dry_run
        self.state_path = state_path

        self._load_settings(settings_path)
        self._load_state(state_path)
        if self.dry_run:
            self.db = None
            self._print_dr("Connecting to db.")
        else:
            self.db = psycopg2.connect(
                    database=self.settings['database']['database'],
                    host=self.settings['database']['host'],
                    port=self.settings['database']['port'],
                    user=self.settings['database']['user'],
                    password=self.settings['database']['password'])
        self.export_dir_path = self.settings['paths']['export_dir']

        self.mserver_db = None

    def __del__(self):
        self._save_state(self.state_path)

    def _print_dr(self, s):
        print("[dry-run] %s" % s)

    def export_data(self, end_date=None):
        if not end_date:
            end_date = datetime.datetime.utcnow().date()
        self.generate_export_archives(end_date)
        self.copy_export_archives(end_date)

    def copy_export_archives(self, end_date):
        for task in self.tasks:
            day = task.next_remote_export_date()
            while day <= end_date:
                path = self._tgzfile_path(task, day)
                if not self.dry_run and not os.path.exists(path):
                    raise Exception("baaaaad")
                try:
                    task.scp_to_destination(path, dry_run=self.dry_run)
                except:
                    print("baaaaad")
                    break
                else:
                    task.last_remote_export = day
                    day += datetime.timedelta(days=1)

    def query_measurement_table(self, table, day):
        if self.dry_run:
            self._print_dr(
                    "Querying table '%s' for date %s." %
                    (table, day.isoformat()))
            return [], []

        cur = self.db.cursor()
        cur.execute((
                "SELECT deviceid, "
                "       eventstamp as date,"
                "       srcip, "
                "       dstip, "
                "       direction as dir, "
                "       tool, "
                "       exitstatus, "
                "       average as mean, "
                "       std as stddev, "
                "       minimum as min, "
                "       maximum as max, "
                "       median, "
                "       iqr "
                "FROM %s "
                "WHERE date_trunc('day', eventstamp) = %s "
                "ORDER BY eventstamp;"),
                (table, day))
        header = map(lambda x: x.name, cur.description)
        rows = cur.fetchall()
        return header, rows

    def generate_export_archives(self, end_date):
        table_set = set()

        min_date_req = end_date + datetime.timedelta(days=1)
        for task in self.tasks:
            table_set.update(set(task.filter_tables(self.ALL_TABLES)))
            task_date_req = task.next_local_export_date()
            min_date_req = min(min_date_req, task_date_req)

        day = min_date_req
        while day <= end_date:
            for table in table_set:
                header, rows = self.query_measurement_table(table, day)
                for task in self.tasks:
                    if (task.next_local_export_date() <= day
                            and task.table_required(table)):
                        out_rows = self.filter_table(task, day, rows)
                        csvpath = self._csvfile_path(task, day, table)
                        self.dump_rows_csv(task, csvpath, header, out_rows)
            for task in self.tasks:
                if task.next_local_export_date() <= day:
                    self.create_tgz(task, day)
                    task.last_local_export = day
            day += datetime.timedelta(days=1)

    def filter_table(self, task, day, rows):
        if self.dry_run:
            self._print_dr("Filtering rows by task criteria.")
            return []

        msdb = self.mserver_db
        out_rows = []
        for row in rows:
            if (task.device_required(row[0]) and
                    (task.server_required(msdb.lookup_ptr(row[2], day)) or
                    task.server_required(msdb.lookup_ptr(row[3], day)))):
                out_rows.append(row)
        return out_rows

    def _task_dir(self, task):
        outdir = os.path.abspath(
                os.path.join(self.export_dir_path, task.unique_id))
        return outdir

    def _daily_workdir(self, task, day):
        outdir = os.path.join(self._task_dir(task), day.isoformat())
        return outdir

    def _csvfile_path(self, task, day, table):
        outdir = self._daily_workdir(task, day)
        try:
            os.makedirs(outdir)
        except os.error:
            pass
        csvpath = os.path.join(
                outdir, '%s_%s_%s.csv' %
                (task.unique_id, day.isoformat(), self.FRIENDLY_NAMES[table]))
        return csvpath

    def _tgzfile_path(self, task, day):
        outdir = self._task_dir(task)
        try:
            os.makedirs(outdir)
        except os.error:
            pass
        tgzpath = os.path.join(
                outdir, '%s_%s.tar.gz' % (task.unique_id, day.isoformat()))
        return tgzpath

    def dump_rows_csv(self, task, path, header, rows):
        if self.dry_run:
            self._print_dr("Creating csv file '%s'." % path)
            return

        f = open(path, 'w')
        csvwriter = csv.writer(f)
        start_index = 0 if task.include_deviceid else 1
        csvwriter.writerow(header[start_index:])
        csvwriter.writerows(map(lambda x: x[start_index:], rows))
        f.close()
        os.fsync()

    def create_tgz(self, task, day):
        if self.dry_run:
            tgzpath = self._tgzfile_path(task, day)
            self._print_dr("Creating archive file '%s'." % tgzpath)
            return tgzpath

        outdir = self._daily_workdir(task, day)
        paths = os.listdir(outdir)
        assert(len(paths) > 0)
        tgzpath = self._tgzfile_path(task, day)
        with tarfile.open(tgzpath, 'w:gz') as tf:
            for path in paths:
                tf.add(path)
        os.fsync()
        os.remove(outdir)
        return tgzpath

    def _load_state(self, path):
        try:
            self.state = json.load(open(os.path.abspath(path), 'r'))
        except (ValueError, IOError):
            pass
        else:
            for task in self.tasks:
                task.load_state(self.state.get(task.unique_id, None))

    def _save_state(self, path):
        for task in self.tasks:
            task.dump_state(self.state.setdefault(task.unique_id, {}))

        dthandler = lambda obj: obj.isoformat() if hasattr(obj, 'isoformat') \
                    else TypeError("Cannot serialize.")

        if self.dry_run:
            self._print_dr(
                    "Saving state:\n%s" %
                    json.dumps(self.state, indent=2, default=dthandler))
        else:
            json.dump(
                    self.state, open(os.path.abspath(path), 'w'), indent=2,
                    default=dthandler)

    def _load_settings(self, path):
        self.settings = json.load(open(os.path.abspath(path), 'r'))
        self._load_tasks()

    def _load_tasks(self):
        for json_task in self.settings['tasks']:
            task = ExportTask(json_task)
            if task.is_valid():
                self.tasks.append(task)


def main():
    ap = argparse.ArgumentParser(
            description="Copy data to reqeusting parties (i.e. M-Lab).")
    ap.add_argument(
            '--dry-run', dest='dry_run', action='store_true', default=False,
            help="print actions but don't actually do anything")
    ap.add_argument('settings_path', metavar='SETTINGS_FILE.json')
    ap.add_argument('state_path', metavar='STATE_FILE.json')
    args = ap.parse_args()
    ep = ExportProcessor(args.state_path, args.settings_path, args.dry_run)
    ep.export_data()


if __name__ == '__main__':
    sys.exit(main())














###############################################################################
#    if not (2 <= len(sys.argv) <= 3):
#        print("USAGE %s output_filename.json [DOWNTIME_THRESHOLD=180]"
#              % sys.argv[0])
#        sys.exit(2)
#    f = open(sys.argv[1], 'w')
#    if len(sys.argv) == 3:
#        DOWNTIME_THRESHOLD = datetime.timedelta(seconds=int(sys.argv[2]))
#    else:
#        DOWNTIME_THRESHOLD = datetime.timedelta(seconds=180)
#
#    config = {}
#    for evname in REQ_ENV_VARS:
#        try:
#            config[evname] = os.environ[evname]
#        except KeyError:
#            print(("Environment variable '%s' required and not defined. "
#                    "Terminating.") % evname)
#            sys.exit(1)
#    for (evname, default_val) in OPT_ENV_VARS:
#        config[evname] = os.environ.get(evname) or default_val
#
#    mconn = psycopg2.connect(
#            host=config['BDM_PG_HOST'],
#            port=int(config['BDM_PG_PORT']),
#            database=config['BDM_PG_MGMT_DBNAME'],
#            user=config['BDM_PG_USER'],
#            password=config['BDM_PG_PASSWORD'],
#            )
#
#    mcur = mconn.cursor()
#    mcur.execute(
#            "SELECT id, date_seen "
#            "FROM devices_log "
#            "ORDER BY id, date_seen;")
#    data = mcur.fetchall()
#    intervals_by_id = {}
#    current_id = None
#    current_intervals = []
#    interval_start = None
#    interval_end = None

            # dump table for given date
            # for each task with start_date >= day:
            #   filter table names
            #   for each table:
            #       for each row in table, filter/match on deviceid and
            #       src/dst ip, and output to file
        #Pseudo-code solution:
        # precursor -- state file .json lying around somewhere
        #   contains: last_export and last_scp dates for each task
        # compute days_required as the sequence min(k1, k2, ..., kN)..today,
        #       where ki in the max(last_export_i, start_date_i) for the ith
        #       task
        # for each day in days_required sequence:
        #   dump all data in all tables
        #   for each task in tasks:
        #       filter table names and only process matching tables
        #           iterate through each table, outputting rows that match
        #                   device_id and target_id
        # update state file with today's date for each task
        # scp all files in destination dirs for each target with sort order >
        #       last_scp ; for each success, update last_scp state variable
        #       and/or move to archives/'sent' directory
