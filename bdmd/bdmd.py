#!/usr/bin/env python

import calendar
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
                'BDM_PG_MGMT_DBNAME',
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


# lifted from https://github.com/markokr/skytools -- MITish license
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
            #s.ioctl(SIO_KEEPALIVE_VALS,
            #        (1, tcp_keepidle*1000, tcp_keepintvl*1000))
            pass
    else:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)


class DatabaseConnectionException(Exception):
    """
    A problem occurred with the database connection.
    """
    def __init__(self, msg=None):
        self.msg = msg
    def __str__(self):
        return("DatabaseConnectionException: '%s'" % repr(self.msg))



class ClientRequestException(Exception):
    """
    A problem with the request string provided by the client.
    """
    def __init__(self, msg=None):
        self.msg = msg
    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return("ClientRequestException(\"%s\")" % repr(self.msg))


class MeasurementRequest(object):
    def __init__(self, *args, **kwargs):
        self.category = None
        self.type = None
        self.zone = None
        self.duration = None
        self.default_target = False

        if 'payload' in kwargs:
            self.init_payload(kwargs['payload'])
        else:
            self.category = kwargs.get('category')
            self.type = kwargs.get('type')
            self.zone = kwargs.get('zone')
            self.duration = kwargs.get('duration')

    def init_payload(self, payload):
        # probe format: "<probe_id> measure <cat> <type> <zone> <dur>"
        #          e.g. "OW0123456789AB measure Bismark PING NorthAm 0"
        try:
            payload_parts = payload.split(None, 4)
            self.category =  payload_parts[0]
            self.type = payload_parts[1]
            self.zone = payload_parts[2]
            self.duration = int(payload_parts[3])
        except (IndexError, ValueError):
            raise ClientRequestException(
                    "Measurement request '%s' incorrectly formatted" % payload)


class Probe(object):
    def __init__(self, probe_str, host):
        parts = probe_str.split(None, 3)
        if len(parts) < 3:
            raise ClientRequestException(
                    "Probe '%s' incorrectly formatted" % probe_str)
        self.id = parts[0]
        self.cmd = parts[1]
        self.param = parts[2]
        try:
            self.payload = parts[3].strip()
        except IndexError:
            self.payload = None
        self.ip = host
        self.arrival_time = datetime.datetime.utcnow()
        self.blacklisted = False
        self.reply = None


def eb_print(x):
    print("errback!")
    print(x, x.value)


class ProbeHandler(DatagramProtocol):
    def __init__(self, config):
        txpostgres.Connection.connectionFactory = self._tcp_connfactory({
                'tcp_keepidle'  : int(config['BDMD_TCP_KEEPIDLE']),
                'tcp_keepcnt'   : int(config['BDMD_TCP_KEEPCNT']),
                'tcp_keepintvl' : int(config['BDMD_TCP_KEEPINTVL']),
                })
        self.dbpool = txpostgres.ConnectionPool(
                None,
                min=int(config['BDMD_TXPG_CONNPOOL']),
                host=config['BDM_PG_HOST'],
                port=int(config['BDM_PG_PORT']),
                database=config['BDM_PG_MGMT_DBNAME'],
                user=config['BDM_PG_USER'],
                password=config['BDM_PG_PASSWORD'],
                )
        self.dbpool_started = False
        self.config = {}
        self.config['logdir'] = os.path.join(
                os.path.abspath(config['VAR_DIR']), LOG_SUBDIR)
        self.config['max_delay'] = int(config['BDMD_MAX_DELAY'])
        self.config['time_error'] = int(config['BDMD_TIME_ERROR'])

    def datagramReceived(self, data, (host, port)):
        # TODO maybe this should be done with a queue instead
        try:
            p = Probe(data, host)
        except ClientRequestException as cre:
            print(cre)
            return

        print_debug("%s - \"%s %s\" from %s [%s]" %
                (p.arrival_time.isoformat(), p.cmd, p.param, p.id, host))

        d = self.check_blacklist(p)
        d.addCallback(self.dispatch_response)
        d.addCallback(self.send_reply, (host, port))
        d.addErrback(self.db_error_handler)
        d.addErrback(self.client_error_handler)
        #d.addErrback(eb_print)

    @print_entry
    def db_error_handler(self, failure):
        # TODO: http://archives.postgresql.org/psycopg/2011-02/msg00039.php
        failure.trap(psycopg2.Error)
        print("trapped psycopg2.Error")
        print(dir(failure.value))
        print(failure.value)

    @print_entry
    def client_error_handler(self, failure):
        failure.trap(ClientRequestException)
        print("trapped ClientRequestException")
        print(failure.value)

    @print_entry
    def check_blacklist(self, probe):
        d = self.dbpool.runQuery(
                "SELECT device_id FROM blacklist where device_id=%s;",
                        [probe.id])
        return d.addCallback(self.check_blacklist_qh, probe)

    @print_entry
    def check_blacklist_qh(self, resultset, probe):
        # TODO being on the blacklist could be handled with an errback with a
        #      special blacklist exception...
        if resultset:
            probe.blacklisted = True
        return defer.succeed(probe)

    @print_entry
    def dispatch_response(self, probe):
        d = None
        if not probe.blacklisted:
            if probe.cmd == 'ping':
                d = self.handle_ping_req(probe)
            elif probe.cmd == 'log':
                d = self.handle_log_req(probe)
            elif probe.cmd == 'measure':
                d = self.handle_measure_req(probe)
            elif probe.cmd == 'echo':
                probe.reply = (
                        r"echo_reply: '%s'" %
                        ' '.join((probe.id, probe.cmd, probe.param)))
                d = defer.succeed(probe)
        return d

    @print_entry
    def send_reply(self, probe, (host, port)):
        if probe and probe.reply:
            self.transport.write("%s" % probe.reply, (host, port))

    @print_entry
    def handle_log_req(self, probe):
        print("%s - Received log from %s: %s" %
                (probe.arrival_time.isoformat(), probe.id, probe.param))

        try:
            # write log entry
            logfilename = os.path.basename('%s.log' % probe.id)
            logfile = open(
                    os.path.join(self.config['logdir'], logfilename), 'a')
            logfile.write("%s - %s\n%s\nEND - %s\n" %
                    (probe.arrival_time.isoformat(), probe.param,
                    probe.payload, probe.param))
            logfile.close()
        except IOError as ioe:
            # TODO should this be return defer.fail(ioe)?
            #      or should this amount to the same thing?
            raise ioe
        finally:
            # send message to bdm client
            d = self.dbpool.runOperation(
                    ("INSERT INTO messages (msgfrom, msgto, msg) "
                    "VALUES (%s, 'BDM', %s);"), [probe.id, probe.param])
        return d.addCallback(lambda _: None)

    @print_entry
    def handle_measure_req(self, probe):
        try:
            # in the case of measurement request probes, the payload actually
            # includes the 'param' field, so we must join them before sending
            # the measurement probe payload for parsing.
            mreq = MeasurementRequest(
                    payload=' '.join([probe.param, probe.payload]))
        except ClientRequestException as cre:
            return defer.fail(cre)

        d = self.dbpool.runQuery((
                "SELECT * "
                "FROM device_targets as dt "
                "WHERE dt.device_id = %s;"),
                [probe.id])
        return d.addCallback(self.measure_default_target_check, probe, mreq)

    def measure_default_target_check(self, resultset, probe, mreq):
        if resultset:
            return self.dbpool.runInteraction(
                    self.measure_req_interaction, probe, mreq)
        else:
            mreq.default_target = True
            return self.dbpool.runInteraction(
                    self.measure_req_defaultinteraction, probe, mreq)

    @print_entry
    def measure_req_interaction(self, cursor, probe, mreq):
        d = cursor.execute((
                "SELECT t.id, ti.ip, ts.info, t.date_free, t.curr_cli, "
                "   t.max_cli, s.is_exclusive, t.fqdn "
                "FROM targets as t, target_ips as ti, target_services as ts, "
                "   services as s, device_targets as dt "
                "WHERE dt.device_id = %s "
                "   AND t.id = dt.target_id "
                "   AND t.available = TRUE "
                "   AND ti.target_id = dt.target_id "
                "   AND ti.date_effective >= ( "
                "       SELECT max(ti2.date_effective) "
                "       FROM target_ips as ti2 "
                "       WHERE ti2.target_id = ti.target_id ) "
                "   AND ts.target_id = dt.target_id "
                "   AND ts.service_id = s.id "
                "   AND dt.is_enabled = TRUE "
                "   AND s.name = %s "
                "   AND (s.is_exclusive = FALSE OR "
                "        (s.is_exclusive = TRUE AND t.date_free < %s)) "
                "ORDER BY dt.preference DESC, t.date_free ASC "
                "LIMIT 1 "
                "FOR UPDATE OF t;"  # N.B. 'FOR UPDATE' locks the selected row
                                    #      for the coming update below
                ), [
                probe.id,
                mreq.type,
                (probe.arrival_time +
                datetime.timedelta(seconds=self.config['max_delay'])),
                ])
        return d.addCallback(self.measure_req_qh, probe, mreq)

    @print_entry
    def measure_req_defaultinteraction(self, cursor, probe, mreq):
        d = cursor.execute((
                "SELECT t.id, ti.ip, ts.info, t.date_free, t.curr_cli, "
                "   t.max_cli, s.is_exclusive, t.fqdn "
                "FROM targets as t, target_ips as ti, target_services as ts, "
                "   services as s "
                "WHERE t.fqdn = 'porter-square.cc.gt.atl.ga.us.' "
                "  AND t.available = TRUE "
                "  AND ti.target_id = t.id"
                "  AND ti.date_effective = ( "
                "      SELECT max(ti2.date_effective) "
                "      FROM target_ips as ti2 "
                "      WHERE ti2.target_id = ti.target_id "
                "      GROUP BY ti2.target_id) "
                "  AND ts.target_id = t.id"
                "  AND ts.service_id = s.id"
                "  AND s.name = %s "
                "  AND (s.is_exclusive = FALSE OR "
                "       (s.is_exclusive = TRUE AND t.date_free < %s)) "
                "ORDER BY t.date_free ASC "
                "LIMIT 1 "
                "FOR UPDATE OF t;"  # N.B. 'FOR UPDATE' locks the selected row
                                    #      for the coming update below
                ), [
                mreq.type,
                (probe.arrival_time +
                datetime.timedelta(seconds=self.config['max_delay'])),
                ])
        return d.addCallback(self.measure_req_qh, probe, mreq)

    @print_entry
    def measure_req_qh(self, cursor, probe, mreq):
        d = None
        resultset = cursor.fetchone()
        if resultset:
            # time_error is a correction factor for processing & comm. time
            measure_start = (probe.arrival_time +
                    datetime.timedelta(seconds=self.config['time_error']))
            delay = datetime.timedelta()

            # these are all target ("t_") characteristics
            t_id        = int(resultset[0])
            t_ip        = resultset[1]
            t_info      = resultset[2]
            t_date_free = resultset[3]
            t_curr_cli  = int(resultset[4])
            t_max_cli   = int(resultset[5])
            t_exclusive = (resultset[6] == True)
            t_fqdn      = resultset[7]

            if t_exclusive:
                if t_date_free > probe.arrival_time:
                    delay = t_date_free - probe.arrival_time
                    measure_start += delay
                d = cursor.execute(
                        "UPDATE targets SET date_free=%s WHERE id=%s;",
                        [measure_start +
                        datetime.timedelta(seconds=mreq.duration),
                        t_id])
                d.addCallback(lambda _: probe)
            else:
                d = defer.succeed(probe)
            probe.reply = '%s %s %d\n' % (t_ip, t_info, delay.seconds)
            print(("%s - Scheduled %s measure from %s to %s (%s) at %s for %d "
                    "seconds%s" % (
                    probe.arrival_time.isoformat(),
                    mreq.type,
                    probe.id,
                    t_fqdn,
                    t_ip,
                    measure_start.isoformat(),
                    mreq.duration,
                    " DEFAULT_TARGET" if mreq.default_target else "")))
        else:
            probe.reply = ' '
            print("%s - No target available for %s measurement from %s" %
                    (probe.arrival_time.isoformat(), mreq.type, probe.id))
            d = defer.succeed(probe)
        return(d)

    @print_entry
    def handle_ping_req(self, probe):
        d = self.register_device(probe)
        d.addCallback(self.check_messages)
        d.addCallback(self.prepare_reply, probe)
        return(d)

    @print_entry
    def register_device(self, probe):
        d = self.dbpool.runQuery(
                "SELECT id FROM devices where id=%s;", [probe.id])
        return d.addCallback(self.register_device_qh, probe)

    @print_entry
    def register_device_qh(self, resultset, probe):
        if resultset:
            query = ("UPDATE devices SET ip=%s, date_last_seen=%s, bversion=%s "
                    "WHERE id=%s;")
        else:
            query = ("INSERT INTO devices (ip, date_last_seen, bversion, id) "
                    "VALUES (%s, %s, %s, %s);")
        d = self.dbpool.runOperation(
                query, [probe.ip, probe.arrival_time, probe.param, probe.id])
        return d.addCallback(lambda _: probe)

    @print_entry
    def check_messages(self, probe):
        d = self.dbpool.runQuery(("SELECT id, msgfrom, msgto, msg "
                "FROM messages WHERE msgto=%s LIMIT 1;"), [probe.id])
        return d.addCallback(self.check_messages_qh)

    def check_messages_qh(self, resultset):
        if resultset:
            msg_id = resultset[0][0]
            msg = resultset[0][3]
            da = self.dbpool.runOperation(
                    "DELETE FROM messages where id=%s;", [msg_id])
            return da.addCallback(lambda _: msg)
        else:
            return defer.succeed(None)

    def prepare_reply(self, message, probe):
        if message:
            probe.reply = message
        else:
            probe.reply = "pong %s %d" % (
                    probe.ip, calendar.timegm(probe.arrival_time.timetuple()))
        return defer.succeed(probe)

    def stop(self):
        print_debug("Shutting down...")
        self.dbpool.close()

    def start(self):
        print_debug("Starting up...")
        d = self.dbpool.start()
        d.addCallbacks(self.started, self.start_failed)
        #reactor.callLater(30, self.check_started)

    def started(self, _):
        print("Database connection pool started!")
        self.dbpool_started = True

    def start_failed(self, failure):
        print(("Database connection pool startup timed out. Terminating."))
        print(failure.value.subFailure)
        reactor.stop()

    @staticmethod
    def _tcp_connfactory(params):
        def connect(*args, **kwargs):
            conn = psycopg2.connect(*args, **kwargs)
            set_tcp_keepalive(conn.fileno(),
                              tcp_keepidle=params['tcp_keepidle'],
                              tcp_keepcnt=params['tcp_keepcnt'],
                              tcp_keepintvl=params['tcp_keepintvl'])
            return conn
        return staticmethod(connect)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_error("  USAGE: %s PORT..." % sys.argv[0])
        sys.exit(1)

    conf = {}
    for evname in REQ_ENV_VARS:
        try:
            conf[evname] = os.environ[evname]
        except KeyError:
            print_error(("Environment variable '%s' required and not defined. "
                         "Terminating.") % evname)
            sys.exit(1)
    for (evname, default_val) in OPT_ENV_VARS:
        conf[evname] = os.environ.get(evname) or default_val

    print_debug = print_debug_factory(int(conf['BDMD_DEBUG']) != 0)
    print_debug(conf)

    probehandlers = []
    for port in (int(x) for x in sys.argv[1:]):
        if 1024 <= port <= 65535:
            ph = ProbeHandler(conf)
            reactor.listenUDP(port, ph)
            probehandlers.append(ph)
            print("Listening on port %d" % port)
        else:
            print_error("Invalid port %d" % port)
    if len(probehandlers) == 0:
        print_error("Not listening on any ports. Terminating.")
        sys.exit(1)
    for ph in probehandlers:
        reactor.addSystemEventTrigger('before', 'startup', ph.start)
        reactor.addSystemEventTrigger('before', 'shutdown', ph.stop)
    reactor.run()
