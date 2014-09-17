#!/usr/bin/env python3
# encoding: utf-8

import cmd
import sys
import psycopg2
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('/etc/pdnscmd.conf')

try:
    MASTER_DNS = config.get('global', 'master_dns')
except configparser.NoOptionError:
    MASTER_DNS = 'example.com'
try:
    SLAVES = [x.strip() for x in config.get('global', 'slaves').split(',')]
except configparser.NoOptionError:
    SLAVES = ['ns2.example.com']
try:
    ADMIN_CONTACT = config.get('global', 'admin_contact')
except configparser.NoOptionError:
    ADMIN_CONTACT = 'hostmaster.example.com'

try:
    dbname = config.get('postgres', 'database')
except configparser.NoOptionError:
    dbname = 'postgres'
try:
    dbuser = config.get('postgres', 'user')
except configparser.NoOptionError:
    dbuser = 'powerdns'
try:
    dbhost = config.get('postgres', 'host')
except configparser.NoOptionError:
    dbhost = '127.0.0.1'
try:
    password = config.get('postgres', 'password')
except configparser.NoOptionError:
    password = None
    f = open("/etc/powerdns/pdns.d/pdns.local.gpgsql",'r')
    for line in f.readlines():
        if line.startswith('gpgsql-password='):
            password = line.split('=',1)[1]
    f.close()

    if not password:
        print("Cannot find postgres password")
        sys.exit(1)

dbconn = conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s" % (dbname, dbuser, password, dbhost))
db = conn.cursor()

class CommandException(Exception):
    pass

class Task(object):
    def validate(self):
        return True

    def execute(self):
        return True

class Record(Task):
    def __init__(self, key, rtype, value, ttl, priority, weight, port, domain, delete=False):
        if key == '@':
            self.key = domain.domain
        else:
            self.key = "%s.%s" % (key, domain.domain)
        self.rtype = rtype
        self.value = value
        self.ttl = ttl
        self.priority = priority
        self.weight = weight
        self.port = port
        self.domain = domain
        self.delete = delete

    def execute(self):
        args = ['key','type', 'value', 'zone_id']
        values = [self.key, self.rtype, self.value, self.domain.zone_id]
        for k, v in (('ttl', self.ttl), ('priority', self.priority), ('weight', self.weight), ('port', self.port)):
            if v is not None:
                args.append(k)
                values.append(v)

        if self.delete:
            db.execute("DELETE FROM dns_records WHERE " + ' and '.join([ "%s=%%s" % k for k in args]) + " RETURNING id", values)
        else:
            db.execute("INSERT INTO dns_records (" + ', '.join(args) + ") VALUES (" + ','.join(['%s']*len(values)) + ") RETURNING id", values)
        if db.fetchone():
            return True
        return False


class Domain(Task):
    def __init__(self, domain):
        self.domain = domain
        self.zone_id = None

    def validate(self):
        if ' ' in self.domain or '.' not in self.domain:
            raise CommandException("Invalid domain %s" % self.domain)
        return True

    def exists(self):
        db.execute("SELECT name, id from dns_zones where name = %s", (self.domain,))
        res = db.fetchone()
        if res:
            self.zone_id = int(res[1])
            return True
        else:
            return False

    def exists_record(self, key, rtype, value, priority=None, weight=None, port=None):
        if key == '@':
            key = self.domain
        else:
            key = '%s.%s' % (key, self.domain)
        query = "SELECT id from dns_records WHERE zone_id = %s and key = %s and type = %s and value = %s"
        args = [self.zone_id, key, rtype, value]
        if priority is not None:
            query = "%s and priority = %s"
            args.append(priority)
        if weight is not None:
            query = "%s and weight = %s"
            args.append(weight)
        if port is not None:
            query = "%s and port = %s"
            args.append(port)
        db.execute(query, args)
        if db.fetchone() is None:
            return False
        return True

    def create(self):
        if self.exists():
            return
        db.execute("INSERT INTO dns_zones (name, rname, refresh, retry, expire, ttl, negative_ttl, nameservers, last_check, notified_serial, type, master) VALUES (%s, %s, 3600 ,900, 68400, 360, 900, %s, NULL, 0, 'MASTER', %s) RETURNING id", (self.domain, ADMIN_CONTACT, [MASTER_DNS] + SLAVES, MASTER_DNS))
        res = db.fetchone()
        self.zone_id = int(res[0])
        db.execute("INSERT INTO dns_records (key, type, ttl, value, zone_id) VALUES (%s, 'SOA', 360, %s, %s)" , (self.domain, '%s %s %s01 3600 900 1209600 86400' % (MASTER_DNS, ADMIN_CONTACT, datetime.now().strftime("%Y%m%d")), self.zone_id))
        for i in [MASTER_DNS] + SLAVES:
            db.execute("INSERT INTO dns_records (key, type, ttl, value, zone_id) VALUES (%s, 'NS', 360, %s, %s)" , (self.domain, i, self.zone_id))

    def inc_serial(self):
        db.execute("SELECT id,value FROM dns_records WHERE type = 'SOA' and zone_id = %s", (self.zone_id,))
        res = db.fetchone()
        cur = res[1]
        serial = int(cur.split()[2])+1
        alt = int(datetime.now().strftime('%Y%m%d01'))
        if alt > serial:
            serial = alt
        soa = "%s %s %s %s %s %s %s" % tuple(cur.split()[:2] + [serial] + cur.split()[3:])
        db.execute("UPDATE dns_records SET value = %s WHERE id = %s", (soa, res[0]))

    def execute(self):
        self.create()


class DNSCommander(cmd.Cmd):
    prompt = '> '

    todoqueue = []
    current_domain = None
    update_serial = False

    def do_domain(self, line):
        """Select and add new domain"""
        d = Domain(line)
        d.validate()
        if not d.exists():
            self.todoqueue.append(d)
        print("Domain: %s" % line)
        self.current_domain = d
        self.prompt = '%s> ' % line

    def complete_domain(self, line, text, begidx, endidx):
        completions = []
        db.execute("SELECT name from dns_zones WHERE name LIKE %s", ('%s%%' % line.strip(),))
        for res in db.fetchall():
            if res[0].startswith(line.strip()):
                completions.append(res[0])
        if len(completions) > 20:
            return []
        return completions

    def reset_prompt(self):
        self.update_serial = False
        self.current_domain = None
        self.prompt = '> '

    def do_commit(self, line):
        """Commit changes"""
        for t in self.todoqueue:
            t.execute()
        if self.update_serial:
            self.current_domain.inc_serial()
        dbconn.commit()
        self.reset_prompt()


    def do_revert(self, line):
        self.todoqueue = []
        dbconn.rollback()
        self.reset_prompt()

    def parse_ttl(self, ttl):
        try:
            ttl = int(ttl)
            if ttl > 0 and ttl < 65535:
                return ttl
        except ValueError:
            pass
        raise CommandException("Invalid ttl %s" % ttl)

    def parse_weight(self, weight):
        try:
            weight = int(weight)
            if weight > 0 and weight < 65535:
                return weight
        except ValueError:
            pass
        raise CommandException("Invalid weight %s" % weight)

    def parse_priority(self, t):
        try:
            t = int(t)
            if t > 0 and t < 65535:
                return t
        except ValueError:
            pass
        raise CommandException("Invalid priority %s" % t)

    def parse_port(self, t):
        try:
            t = int(t)
            if t > 0 and t < 65535:
                return t
        except ValueError:
            pass
        raise CommandException("Invalid port %s" % t)

    def parse_record(self, line):
        ttl = 360
        priority = None
        weight = None
        port = None
        parts = line.split(None, 6)
        if len(parts) < 3:
            raise CommandException("Cannot parse %s" % line)
        key = parts[0].strip()
        record_type = parts[1].strip()
        if parts[1] in ['TXT','A','AAAA','NS', 'CNAME']:
            parts = line.split(None, 3)
            if len(parts) == 4:
                ttl = self.parse_ttl(parts[2])
                value = parts[3]
            elif len(parts) == 3:
                value = parts[2]
            else:
                raise CommandException("Cannot parse %s" % line)
        elif parts[1] in ['MX']:
            parts = line.split(None, 4)
            if len(parts) == 5:
                ttl = self.parse_ttl(parts[2])
                weight = self.parse_weight(parts[3])
                value = parts[4]
            elif len(parts) == 4:
                weight = self.parse_weight(parts[2])
                value = parts[3]
            else:
                raise CommandException("Cannot parse %s" % line)
        elif parts[1] not in ['SRV']:
            if len(parts) == 7:
                ttl = self.parse_ttl(parts[2])
                priority = self.parse_priority(parts[3])
                weight = self.parse_weight(parts[4])
                port = self.parse_weight(parts[5])
                value = parts[6]
            elif len(parts) == 6:
                priority = self.parse_priority(parts[2])
                weight = self.parse_weight(parts[3])
                port = self.parse_weight(parts[4])
                value = parts[5]
            elif len(parts) == 5:
                weight = self.parse_weight(parts[2])
                port = self.parse_weight(parts[3])
                value = parts[4]
            else:
                raise CommandException("Cannot parse %s" % line)
        else:
            parts = line.split(None,5)
            if len(parts) == 6:
                ttl = self.parse_ttl(parts[2])
                priority = self.parse_priority(parts[3])
                weight = self.parse_weight(parts[4])
                value = parts[5]
            elif len(parts) == 5:
                priority = self.parse_priority(parts[2])
                weight = self.parse_weight(parts[3])
                value = parts[4]
            elif len(parts) == 4:
                weight = self.parse_weight(parts[2])
                value = parts[3]
            else:
                raise CommandException("Cannot parse %s" % line)

        if not self.current_domain:
            raise CommandException("Select domain first!")
        return (key, record_type, value, ttl, priority, weight, port)

    def do_add(self, line):
        """
        Add new dns record to zone:

           add key type [ttl] [priority] [weight] [port] value

        Key is for example www
        type is record type, one of A, AAAA, CNAME, TXT, NS, MX, SRV
        ttl is opional time to live value
        priority is used with MX and SRV records
        weight and port are SRV specific values
        """
        key, record_type, value, ttl, priority, weight, port = self.parse_record(line)
        if self.current_domain.exists_record(key, record_type, value, priority=priority, weight=weight, port=port):
            raise CommandException("Record already exists!")

        r = Record(key, record_type, value, ttl=ttl, priority=priority, weight=weight, port=port, domain=self.current_domain)
        self.todoqueue.append(r)
        self.update_serial = True

    def do_delete(self, line):
        """Delete dns record:

            delete key type [ttl] [priority] [weight] [port] value
        """
        key, record_type, value, ttl, priority, weight, port = self.parse_record(line)
        if not self.current_domain.exists_record(key, record_type, value, priority=priority, weight=weight, port=port):
            raise CommandException("Record does not exists!")
        r = Record(key, record_type, value, ttl=ttl, priority=priority, weight=weight, port=port, domain=self.current_domain, delete=True)
        self.todoqueue.append(r)
        self.update_serial = True

    def do_EOF(self, line):
        if self.current_domain:
            self.current_domain = None
            print("")
            self.prompt = '> '
        else:
            print("")
            return True

    def onecmd(self, str):
        try:
            return cmd.Cmd.onecmd(self, str)
        except CommandException as e:
            print('Error: %s' % e)


    def do_list(self, line):
        """List Domains/records"""
        if self.current_domain:
            db.execute("SELECT key, type, ttl, coalesce(priority::text,''), value FROM dns_records WHERE zone_id = %s ORDER BY key, type, value", (self.current_domain.zone_id,))
            for row in db.fetchall():
                print("{0:<20} {2:<6} {1:<5} {3:>4} {4}".format(*row))
        else:
            db.execute("SELECT name, notified_serial FROM dns_zones ORDER BY name")
            for row in db.fetchall():
                print("{0:<20} {1:>12}".format(*row))

if __name__ == '__main__':
    DNSCommander().cmdloop()
