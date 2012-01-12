#!/usr/bin/env python

import time
import datetime
import os
import socket
import sys

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor, defer
from txpostgres import txpostgres
import psycopg2


REQ_ENV_VARS = ['VAR_DIR',
                'BDM_PG_HOST',
                'BDM_PG_USER',
                'BDM_PG_PASSWORD',
                'BDM_PG_DBNAME',
                ]
OPT_ENV_VARS = ['BDM_PG_PORT',
                'BDM_TXPG_CONNPOOL',
                'BDM_TIME_ERROR',
                'BDM_MAX_DELAY',
                'BDM_DB_QUERY_TIMEOUT',
                'BDM_DB_RECON_TIMEOUT',
                ]
LOG_SUBDIR = 'log/devices'


def set_tcp_keepalive(fd, keepalive = True,
                     tcp_keepidle = 4 * 60,
                     tcp_keepcnt = 4,
                     tcp_keepintvl = 15):
    """Turn on TCP keepalive.  The fd can be either numeric or socket
    object with 'fileno' method.

    OS defaults for SO_KEEPALIVE=1:
     - Linux: (7200, 9, 75) - can configure all.
     - MacOS: (7200, 8, 75) - can configure only tcp_keepidle.
     - Win32: (7200, 5|10, 1) - can configure tcp_keepidle and tcp_keepintvl.
       Python needs SIO_KEEPALIVE_VALS support in socket.ioctl to enable it.

    Our defaults: (240, 4, 15).

    >>> import socket
    >>> s = socket.socket()
    >>> set_tcp_keepalive(s)

    (from https://github.com/markokr/skytools -- MITish license)
    """

    # usable on this OS?
    if not hasattr(socket, 'SO_KEEPALIVE') or not hasattr(socket, 'fromfd'):
        return

    # get numeric fd and cast to socket
    if hasattr(fd, 'fileno'):
        fd = fd.fileno()
    s = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)

    # skip if unix socket
    if type(s.getsockname()) != type(()):
        return

    # turn on keepalive on the connection
    if keepalive:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, 'TCP_KEEPCNT'):
            s.setsockopt(socket.IPPROTO_TCP,
                    getattr(socket, 'TCP_KEEPIDLE'), tcp_keepidle)
            s.setsockopt(socket.IPPROTO_TCP,
                    getattr(socket, 'TCP_KEEPCNT'), tcp_keepcnt)
            s.setsockopt(socket.IPPROTO_TCP,
                    getattr(socket, 'TCP_KEEPINTVL'), tcp_keepintvl)
        elif hasattr(socket, 'TCP_KEEPALIVE'):
            s.setsockopt(socket.IPPROTO_TCP,
                    getattr(socket, 'TCP_KEEPALIVE'), tcp_keepidle)
        elif sys.platform == 'darwin':
            TCP_KEEPALIVE = 0x10
            s.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, tcp_keepidle)
        elif sys.platform == 'win32':
            #s.ioctl(SIO_KEEPALIVE_VALS, (1, tcp_keepidle*1000, tcp_keepintvl*1000))
            pass
    else:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)




class Probe(object):

    def __init__(self, probe_str, host):
        parts = probe_str.split()
        if len(parts) < 3:
            raise ValueError()
        self.id = parts[0]
        self.cmd = parts[1]
        self.params = parts[2:]
        self.ip = host
        arrival_time = datetime.datetime.now().replace(microsecond=0)
        self.time_ts = int(time.mktime(arrival_time.timetuple()))
        self.time_str = arrival_time.isoformat()
        self.blacklisted = False
        self.reply = None


class ProbeHandler(DatagramProtocol):

    def __init__(self, config):
        txpostgres.Connection.connectionFactory=self._tcp_connfactory(
                tcp_params=None)
        self.dbpool = txpostgres.ConnectionPool(
                None,
                min=int(config['BDM_TXPG_CONNPOOL'] or 5),
                host=config['BDM_PG_HOST'],
                port=int(config['BDM_PG_PORT'] or 5432),
                database=config['BDM_PG_DBNAME'],
                user=config['BDM_PG_USER'],
                password=config['BDM_PG_PASSWORD'],
                )
        self.config = {}
        self.config['logdir'] = os.path.join(
                os.path.abspath(config['VAR_DIR']), LOG_SUBDIR)
        self.config['max_delay'] = int(config['BDM_MAX_DELAY'] or 300)
        self.config['time_error'] = int(config['BDM_MAX_DELAY'] or 2)

    def datagramReceived(self, data, (host, port)):
        try:
            p = Probe(data, host)
        except ValueError:
            return
        print_debug("%s - \"%s %s\" from %s [%s]" %
                (p.time_str, p.cmd, p.param, p.id, host))

        d = self.check_blacklist(p)

        def handle_probe(probe):
            da = None
            if not probe.blacklisted:
                if probe.cmd == 'ping':
                    da = self.handle_ping(probe)
                elif probe.cmd == 'log':
                    da = self.handle_log(probe)
                elif probe.cmd == 'measure':
                    da = self.handle_measure(probe)
            return da
        d.addCallback(handle_probe)

        def send_reply(probe):
            if probe:
                self.transport.write("%s" % probe.reply, (host, port))
        d.addCallback(send_reply)

    def handle_log(self, probe):
        print(probe.params)
        print("printing log...")  # TODO write log
        d = self.dbpool.runOperation(
                ("INSERT INTO messages (msgfrom, msgto, msg) "
                "VALUES (%s, 'BDM', %s);"), [probe.id,' '.join(probe.params)])
        return d.addCallback(lambda _: None)

    def handle_measure(self, probe):
        m_cat = probe.params[0]   # i.e. Bismark
        m_type = probe.params[1]  # i.e. PING
        m_zone = probe.params[2]  # i.e. NorthAm
        m_dur = int(probe.params[3])   # i.e. 30

        d = self.dbpool.runQuery((
                "SELECT t.ip, c.info, t.free_ts, t.curr_cli, t.max_cli, "
                "       mt.mexclusive "
                "FROM targets as t, capabilities as c, "
                "     device_targets as dt, mtypes as mt "
                "WHERE dt.device = %s "
                "      AND dt.priority >= 0 "
                "      AND dt.server = c.ip "
                "      AND c.service = %s "
                "      AND dt.server = t.ip "
                "      AND t.cat = %s "
                "      AND mt.mtype = c.service "
                "      AND (mt.mexclusive = 0 OR "
                "           (mt.mexclusive = 1 AND "
                "            t.free_ts < %s)) "
                "ORDER BY dt.priority DESC, t.free_ts ASC "
                "LIMIT 1;"), [probe.id, m_type, m_cat,
                probe.time_ts + self.config['max_delay']])

        def handle_query(resultset):
            da = None
            if resultset:
                t_ip        = resultset[0][0]
                t_info      = resultset[0][1]
                t_free_ts   = int(resultset[0][2])
                t_curr_cli  = int(resultset[0][3])
                t_max_cli   = int(resultset[0][4])
                t_exclusive = (int(resultset[0][5]) == 1)

                delay = 0
                if t_exclusive:
                    if t_free_ts > probe.time_ts:
                        delay = t_free_ts - probe.time_ts
                    da = self.dbpool.runOperation(
                            "UPDATE targets SET free_ts=%s WHERE ip=%s;",
                            [probe.time_ts + delay + m_dur +
                            self.config['time_error'], t_ip])

                    da.addCallback(lambda _: probe)
                probe.reply = '%s %s %d\n' % (t_ip, t_info, delay)
                print(("%s - Scheduled %s measure from %s to %s at %d for %s "
                        "seconds" % (probe.time_str, m_type, probe.id, t_ip,
                        probe.time_ts + delay + 10, m_dur)))
                if not da:
                    da = defer.succeed(probe)
            else:
                probe.reply = ' '
                print("%s - No target available for %s measurement from %s" %
                        (probe.time_str, m_type, probe.id))
                da = defer.succeed(probe)
            return(da)
        return d.addCallback(handle_query)

    def check_blacklist(self, probe):
        d = self.dbpool.runQuery(
                "SELECT id FROM blacklist where id=%s;", [probe.id])
        def handle_query(resultset):
            if resultset:
                probe.blacklisted = True
            return defer.succeed(probe)
        return d.addCallback(handle_query)

    def register_device(self, probe):
        d = self.dbpool.runQuery(
                "SELECT id FROM devices where id=%s;", [probe.id])
        def handle_query(resultset):
            if resultset:
                query = ("UPDATE devices SET ip=%s, ts=%s, bversion=%s "
                        "WHERE id=%s;")
            else:
                query = ("INSERT INTO devices (ip, ts, bversion, id) "
                        "VALUES (%s, %s, %s, %s);")
            da = self.dbpool.runOperation(query, [probe.ip, probe.time_ts,
                    probe.params[0], probe.id])
            return da.addCallback(lambda _: probe)
        return d.addCallback(handle_query)

    def check_messages(self, probe):
        d = self.dbpool.runQuery(("SELECT rowid, msgfrom, msgto, msg "
                "FROM messages WHERE msgto=%s LIMIT 1;"), [probe.id])
        def handle_query(resultset):
            if resultset:
                msg_id = resultset[0][0]
                probe.reply = resultset[0][3]
                da = self.dbpool.runOperation(
                        "DELETE FROM messages where rowid=%s;", [msg_id])
                return da.addCallback(lambda _: probe)
            else:
                return defer.succeed(probe)
        return d.addCallback(handle_query)

    def handle_ping(self, probe):
        d = self.register_device(probe)
        d.addCallback(self.check_messages)
        def prepare_reply(probe):
            if not probe.reply:
                probe.reply = "pong %s %d" % (probe.ip, probe.time_ts)
            return defer.succeed(probe)
        return d.addCallback(prepare_reply)

    def shutdown(self):
        print_debug("Shutting down...")
        self.dbpool.close()

    def startup(self):
        print_debug("Starting up...")
        self.dbpool.start()

    @staticmethod
    def _tcp_connfactory(tcp_params):
        def connect(*args, **kwargs):
            conn = psycopg2.connect(*args, **kwargs)
            set_tcp_keepalive(conn.fileno(),
                              tcp_keepidle=10,
                              tcp_keepcnt=2,
                              tcp_keepintvl=10)
            return conn
        return staticmethod(connect)


def print_debug(s):
    print(s)


def print_error(s):
    sys.stderr.write("%s\n" % s)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_error("  USAGE: %s PORT..." % sys.argv[0])
        sys.exit(1)

    config = {}
    for evname in REQ_ENV_VARS:
        try:
            config[evname] = os.environ[evname]
        except KeyError:
            print_error(("Environment variable '%s' required and not defined. "
                         "Terminating.") % evname)
            sys.exit(1)
    for evname in OPT_ENV_VARS:
        config[evname] = os.environ.get(evname)

    ph = ProbeHandler(config)
    listeners = 0
    for port in (int(x) for x in sys.argv[1:]):
        if 1024 <= port <= 65535:
            reactor.listenUDP(port, ph)
            print("Listening on port %d" % port)
            listeners += 1
        else:
            print_error("Invalid port %d" % port)
    if listeners > 0:
        reactor.addSystemEventTrigger('before', 'startup', ph.startup)
        reactor.addSystemEventTrigger('before', 'shutdown', ph.shutdown)
        reactor.run()
    else:
        print_error("Not listening on any ports. Terminating.")
        sys.exit(1)
