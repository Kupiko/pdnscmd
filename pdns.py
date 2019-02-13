#!/usr/bin/env python3
# encoding: utf-8

import cmd
import sys
import os
import psycopg2
from datetime import datetime
import configparser
from ipaddress import IPv6Address, IPv6Network, IPv4Address, IPv4Network, AddressValueError
import subprocess
import requests

import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.basicConfig()

CONFIG_FILE = os.environ.get("CONFIG_FILE", '/etc/pdnscmd.conf')

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

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
    f = open("/etc/powerdns/pdns.d/pdns.local.gpgsql", 'r')
    for line in f.readlines():
        if line.startswith('gpgsql-password='):
            password = line.split('=', 1)[1]
    f.close()

    if not password:
        print("Cannot find postgres password")
        sys.exit(1)

DEFAULT_TTL=360
DEBUG = False

dbconn = conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s" % (dbname, dbuser, password, dbhost))
db = conn.cursor()

def notify_domain(domain):
    p = subprocess.call(['pdns_control', 'notify', domain], timeout=5)


class CommandException(Exception):
    pass

class Task(object):
    def validate(self):
        return True

    def execute(self):
        return True

    def show(self):
        return ""


class RecordActions(object):
    DELETE=1
    UPDATE=2
    ADD=2


class Record(Task):
    def __init__(self, key, rtype, value, domain, ttl=None, priority=None, action=RecordActions.ADD):
        if key == '@' or key == '':
            self.key = domain.domain
        else:
            self.key = "%s.%s" % (key, domain.domain)
        self.rtype = rtype
        self.value = value
        self.ttl = ttl
        if ttl is None and action == RecordActions.ADD:
            self.ttl = DEFAULT_TTL
        self.priority = priority
        self.domain = domain
        self.action = action

    def execute(self):
        args = ['name', 'type', 'content', 'domain_id']
        values = [self.key, self.rtype, self.value, self.domain.zone_id]
        self.domain.clear_records()
        for k, v in (('ttl', self.ttl), ('prio', self.priority),):
            if v is not None:
                args.append(k)
                values.append(v)

        if self.action == RecordActions.DELETE:
            db.execute("DELETE FROM records WHERE " + ' and '.join(["%s=%%s" % k for k in args]) + " RETURNING id", values)
        elif self.action == RecordActions.ADD:
            db.execute("INSERT INTO records (" + ', '.join(args) + ") VALUES (" + ','.join(['%s']*len(values)) + ") RETURNING id", values)
        else:
            raise NotImplemented("Update not implemented")
        if db.fetchone():
            return True
        return False

    def show(self):
        args = [('name', self.key), ('type', self.rtype), ('content', self.value)]
        for k, v in (('ttl', self.ttl), ('prio', self.priority)):
            if v is not None:
                args.append((k,v))
        record = ' and '.join([ "%s=%s" % (k,v) for k,v in args])
        if self.action == RecordActions.DELETE:
            return "DELETE record %s" % record
        else:
            return "ADD record %s" % record


class Domain(Task):
    def __init__(self, domain):
        self.domain = domain.rstrip('.')
        self._records = []
        self.zone_id = None
        self.exists()
        self.to_delete = False

    def validate(self):
        if ' ' in self.domain or '.' not in self.domain:
            raise CommandException("Invalid domain %s" % self.domain)
        return True

    def exists(self):
        db.execute("SELECT name, id from domains where name = %s", (self.domain,))
        res = db.fetchone()
        if res:
            self.zone_id = int(res[1])
            return True
        else:
            return False

    def show(self):
        if not self.exists():
            return "ADD domain %s" % self.domain
        if self.to_delete:
            return "DELETE domain %s" % self.domain
        return ""

    def clear_records(self):
        self._records = []

    def _format_record(self, row):
        return {
            'key': row[0],
            'type': row[1] or '-',
            'ttl': row[2] or '-',
            'priority': row[3] or '-',
            'value': row[4] or '-'
        }

    def update_records(self):
        db.execute("SELECT name, type, ttl, coalesce(prio::text,''), content FROM records WHERE domain_id = %s ORDER BY name, type, content", (self.zone_id,))
        self._records = [{'key': x[0], 'type': x[1] or '-', 'ttl': x[2] or '-', 'priority': x[3] or '-', 'value': x[4] or '-'} for x in db.fetchall()]

    def records(self):
        if not self._records:
            self.update_records()
        return self._records

    def fqdn(self, key):
        key = key.strip('.')
        if key == '@':
            key = self.domain
        elif not key.endswith(self.domain):
            key = '%s.%s' % (key, self.domain)
        return key.lower()

    def exists_record(self, key, rtype, value, priority=None):
        key = self.fqdn(key)
        query = "SELECT id from records WHERE domain_id = %s and name = %s and type = %s and content = %s"
        args = [self.zone_id, key, rtype, value]
        if priority is not None:
            args.append("%s" % (priority,))
            query = query + " and prio = %s"
        if DEBUG:
            print(query)
        db.execute(query, args)
        if db.fetchone() is None:
            return False
        return True

    def get_records(self, key, rtype=None, value=None):
        key = self.fqdn(key)
        args = [self.zone_id, key]
        query = "SELECT name, type, ttl, coalesce(prio::text,''), content FROM records " \
                "WHERE domain_id = %s and name = %s"
        if rtype is not None:
            query += " type = %s"
            args.append(rtype)
        if value is not None:
            query += " content = %s"
            args.append(value)
        if DEBUG:
            print(query)
        db.execute(query, args)
        return [self._format_record(x) for x in db.fetchall()]

    """
         Column      |          Type          |                      Modifiers                       | Storage  | Stats target | Description
    -----------------+------------------------+------------------------------------------------------+----------+--------------+-------------
     id              | integer                | not null default nextval('domains_id_seq'::regclass) | plain    |              |
     name            | character varying(255) | not null                                             | extended |              |
     master          | character varying(128) | default NULL::character varying                      | extended |              |
     last_check      | integer                |                                                      | plain    |              |
     type            | character varying(6)   | not null                                             | extended |              |
     notified_serial | integer                |                                                      | plain    |              |
     account         | character varying(40)  | default NULL::character varying                      | extended |              |
    """

    """

        Column    |           Type           |                      Modifiers                       | Storage  | Stats target | Description
    --------------+--------------------------+------------------------------------------------------+----------+--------------+-------------
      id          | integer                  | not null default nextval('records_id_seq'::regclass) | plain    |              |
      domain_id   | integer                  |                                                      | plain    |              |
      name        | character varying(255)   | default NULL::character varying                      | extended |              |
      type        | character varying(10)    | default NULL::character varying                      | extended |              |
      content     | character varying(65535) | default NULL::character varying                      | extended |              |
      ttl         | integer                  |                                                      | plain    |              |
      prio        | integer                  |                                                      | plain    |              |
      change_date | integer                  |                                                      | plain    |              |
      disabled    | boolean                  | default false                                        | plain    |              |
      ordername   | character varying(255)   |                                                      | extended |              |
      auth        | boolean                  | default true                                         | plain    |              |

    """

    def create(self):
        if self.exists():
            return
        db.execute("INSERT INTO domains (name, last_check, notified_serial, type, master, account) VALUES (%s, NULL, 0, 'MASTER', %s, '') RETURNING id", (self.domain, MASTER_DNS))
        res = db.fetchone()
        self.zone_id = int(res[0])
        db.execute("INSERT INTO records (name, type, ttl, content, prio, domain_id) VALUES (%s, 'SOA', %s, %s, %s, %s)" , (self.domain, DEFAULT_TTL, '%s %s %s01 3600 900 1209600 86400' % (MASTER_DNS, ADMIN_CONTACT, datetime.now().strftime("%Y%m%d")), '0', self.zone_id))
        for i in [MASTER_DNS] + SLAVES:
            db.execute("INSERT INTO records (name, type, ttl, content, prio, domain_id) VALUES (%s, 'NS', %s, %s, 0, %s)" , (self.domain, DEFAULT_TTL, i, self.zone_id))

    def inc_serial(self):
        db.execute("SELECT id, content FROM records WHERE type = 'SOA' and domain_id = %s", (self.zone_id,))
        res = db.fetchone()
        cur = res[1]
        serial = int(cur.split()[2])+1
        alt = int(datetime.now().strftime('%Y%m%d01'))
        if alt > serial:
            serial = alt
        soa = "%s %s %s %s %s %s %s" % tuple(cur.split()[:2] + [serial] + cur.split()[3:])
        db.execute("UPDATE records SET content = %s WHERE id = %s", (soa, res[0]))

    def delete(self):
        if not self.exists():
            return
        db.execute("DELETE from records where domain_id = %s", (self.zone_id,))
        db.execute("DELETE from domains WHERE name = %s", (self.domain,))

    def execute(self):
        if self.to_delete:
            self.delete()
        else:
            self.create()


class DNSCommander(cmd.Cmd):
    prompt = '> '

    todoqueue = []
    current_domain = None
    update_serial = False

    def do_domain(self, line):
        """Select and add new domain"""
        line = line.rstrip('.')
        d = Domain(line)
        d.validate()
        if not d.exists():
            self.todoqueue.append(d)
        print("Domain: %s" % line)
        self.current_domain = d
        self.prompt = '%s> ' % line

    def complete_domain(self, line, text, begidx, endidx):
        completions = []
        db.execute("SELECT name from domains WHERE name LIKE %s", ('%s%%' % line.strip(),))
        for res in db.fetchall():
            if res[0].startswith(line.strip()):
                completions.append(res[0])
        if len(completions) > 20:
            return []
        return completions

    def generate_reverse(self, ip, name, domain=None):
        name = name.rstrip('.') + '.'
        if ':' in ip:
            try:
                ipobject = IPv6Address(ip)
            except AddressValueError as e:
                raise CommandException("Invalid IPv6 address: %s" % e)
            reverse = '.'.join(ipobject.exploded[::-1].replace(':', '')) + '.ip6.arpa'
        else:
            try:
                ipobject = IPv4Address(ip)
            except AddressValueError as e:
                raise CommandException("Invalid IPv4 address: %s" % e)
            reverse = '.'.join(ipobject.exploded.split('.')[::-1]) + '.in-addr.arpa'
        if not domain:
             for d in self.get_domains():
                if reverse.endswith(d[0]):
                    domain = Domain(d[0])
                    break
        if not domain:
            raise CommandException("No such domain for %s" % reverse)

        for r in  domain.records():
            if r['key'] == reverse:
                raise CommandException("Reverse record for key %s already exists with value %s" % (reverse, r['value']))

        if not reverse.endswith(domain.domain):
            raise CommandException("Wrong zone for this record!")

        reverse = reverse[:len(reverse) - len(domain.domain) - 1]

        if domain.exists_record(reverse, "PTR", name):
            raise CommandException("Record already exists!")

        r = Record(reverse, "PTR", name, domain=domain)
        self.todoqueue.append(r)
        self.update_serial = True

    def delete_reverse(self, ip, name, domain=None):
        name = name.rstrip('.') + '.'
        if ':' in ip:
            try:
                ipobject = IPv6Address(ip)
            except AddressValueError as e:
                raise CommandException("Invalid IPv6 address: %s" % e)
            reverse = '.'.join(ipobject.exploded[::-1].replace(':', '')) + '.ip6.arpa'
        else:
            try:
                ipobject = IPv4Address(ip)
            except AddressValueError as e:
                raise CommandException("Invalid IPv4 address: %s" % e)
            reverse = '.'.join(ipobject.exploded.split('.')[::-1]) + '.in-addr.arpa'
        if not domain:
             for d in self.get_domains():
                if reverse.endswith(d[0]):
                    domain = Domain(d[0])
                    break
        if not domain:
            raise CommandException("No such domain for %s" % reverse)

        for r in  domain.records():
            if r['key'] == reverse and r['value'].rstrip('.') == name.rstrip('.'):

                print("Removing reverse record %s PTR %s" % (r['key'], r['value']))
                r = Record(reverse[:len(reverse) - len(domain.domain) - 1], "PTR", r['value'], ttl=r['ttl'], domain=domain, action=RecordActions.DELETE)
                self.todoqueue.append(r)
                self.update_serial = True
                return


    def reset_prompt(self):
        self.update_serial = False
        self.current_domain = None
        self.prompt = '> '

    def do_commit(self, line):
        """Commit changes"""
        domains = []
        for t in self.todoqueue:
            t.execute()
            if t.domain not in domains and isinstance(t.domain, Domain):
                domains.append(t.domain)
        self.todoqueue = []
        if self.update_serial:
            for d in domains:
                d.inc_serial()
        dbconn.commit()
        #self.reset_prompt()
        if self.update_serial:
            for d in domains:
                notify_domain(d.domain)

    def do_revert(self, line):
        """Revert changes"""
        self.todoqueue = []
        dbconn.rollback()
        #self.reset_prompt()

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
            if t >= 0 and t <= 65535:
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
        ttl = None
        priority = None
        parts = line.split(None, 6)
        if len(parts) < 3:
            raise CommandException("Cannot parse %s" % line)
        key = parts[0].strip().lower()
        record_type = parts[1].strip().upper()
        if record_type in ['TXT', 'A', 'AAAA', 'NS', 'CNAME', 'SPF']:
            parts = line.split(None, 3)
            if len(parts) == 4:
                ttl = self.parse_ttl(parts[2])
                value = parts[3]
            elif len(parts) == 3:
                value = parts[2]
            else:
                raise CommandException("Cannot parse %s" % line)
        elif record_type in ['MX', 'SRV', 'TLSA', 'CAA']:
            parts = line.split(None, 4)
            if len(parts) == 5:
                ttl = self.parse_ttl(parts[2])
                priority = self.parse_priority(parts[3])
                value = parts[4]
            elif len(parts) == 4:
                priority = self.parse_priority(parts[2])
                value = parts[3]
            else:
                raise CommandException("Cannot parse %s" % line)
        elif record_type in ['PTR']:
            parts = line.split(None, 3)
            if len(parts) == 4:
                ttl = self.parse_ttl(parts[2])
                value = parts[3]
            elif len(parts) == 3:
                value = parts[2]
            else:
                raise CommandException("Cannot parse %s" % line)
            if not value.endswith('.'):
                value = "%s." % value
        else:
            raise CommandException("Cannot parse %s" % line)

        if not self.current_domain:
            raise CommandException("Select domain first!")
        if record_type == "A":
            try:
                ipobject = IPv4Address(value)
            except AddressValueError as e:
                raise CommandException("Invalid IPv4 address: %s" % e)
        if record_type == "AAAA":
            try:
                ipobject = IPv6Address(value)
            except AddressValueError as e:
                raise CommandException("Invalid IPv6 address: %s" % e)
        return (key, record_type, value.lower(), ttl, priority)

    def do_add(self, line):
        """
        Add new dns record to zone:

           add key type [ttl] [priority] [weight] [port] value

        Key is for example www
        type is record type, one of A, AAAA, CNAME, TXT, NS, MX, SRV, PTR
        ttl is opional time to live value
        priority is used with MX and SRV records
        weight and port are SRV specific values
        """
        key, record_type, value, ttl, priority = self.parse_record(line)
        key = key.rstrip(".")
        if key.endswith(self.current_domain.domain):
            key = key[:-len(self.current_domain.domain)-1]
        if self.current_domain.exists_record(key, record_type, value, priority=priority):
            raise CommandException("Record already exists!")

        r = Record(key, record_type, value, ttl=ttl, priority=priority, domain=self.current_domain)
        self.todoqueue.append(r)
        self.update_serial = True

        # Generate reverse
        if record_type in ['A', 'AAAA']:
            if self.current_domain.domain not in key:
                key = '%s.%s' % (key, self.current_domain.domain)
            self.generate_reverse(value, key)
            print("Generating reverse record also")

    def do_addrev(self, line):
        """
        Add new reverse dns record to zone
            addrev ip name
        """
        if not self.current_domain:
            raise CommandException("Select domain first")
        if len(line.split()) != 2:
            raise CommandException("Invalid arguments")
        ip, name = line.split(None, 1)
        self.update_serial = True
        self.generate_reverse(ip, name, self.current_domain)

    def do_genrev(self, line):
        """
        Generate reverse records for name
            genrev name
        """
        if not self.current_domain:
            raise CommandException("Select domain first")
        if not line:
            raise CommandException("Name required!")
        for record in self.current_domain.records():
            if record['key'] != line:
                continue
            if record['type'] in ['A', 'AAAA']:
                try:
                    self.generate_reverse(record['value'], record['key'])
                    self.update_serial = True
                except CommandException as e:
                    print("Not doing reverse for %s: %s" % (record['value'], e))

    def do_delete(self, line):
        """Delete dns record:

            delete key type [ttl] [priority] [weight] [port] value
        """
        if len(line.split()) < 3:
            print("Use deleteall to delete multiple records")
            return
        key, record_type, value, ttl, priority = self.parse_record(line)
        key = key.rstrip(".").lower()
        if key.endswith(self.current_domain.domain):
            key = key[:-len(self.current_domain.domain)-1].strip()
        if key == '':
            key = '@'
        if not self.current_domain.exists_record(key, record_type, value, priority=priority):
            print("key: '%s' type: '%s' value: '%s' priority: '%s'" % (key, record_type, value, priority))
            if not self.current_domain.exists_record(key, record_type, "%s %s" % (priority, value), priority=None):
                raise CommandException("Record does not exists!")
            value = "%s %s" % (priority, value)
            priority = None
        r = Record(key, record_type, value, ttl=ttl, priority=priority, domain=self.current_domain, action=RecordActions.DELETE)
        self.todoqueue.append(r)
        self.update_serial = True

        if record_type in ['A', 'AAAA']:
            if self.current_domain.domain not in key:
                key = '%s.%s' % (key, self.current_domain.domain)
            self.delete_reverse(value, key)

    def do_deleteall(self, line):
        """Delete all dns records matching key:

           deleteall key [type]
        """
        type = None
        if len(line.split()) == 1:
            key = line.strip()
        else:
            key, type = line.split(None, 1)
            type = type.upper()
        key = key.rstrip(".").lower()
        if key.endswith(self.current_domain.domain):
            key = key[:-len(self.current_domain.domain) - 1].strip()
        if key == '' or key == '@':
            raise CommandException("Removing all from root level is not allowed.")

        for row in self.current_domain.get_records(key=key, rtype=type):
            if row["type"].upper() in ["SOA"]:
                print("Skipping SOA record")
                continue
            row_key = row['key']
            if row_key.endswith(self.current_domain.domain):
                row_key = row_key[:-len(self.current_domain.domain) - 1].strip()
            r = Record(row_key, row["type"], row["value"], ttl=row["ttl"], priority=None if row["priority"] == '-' else row["priority"],
                       domain=self.current_domain, action=RecordActions.DELETE)
            self.todoqueue.append(r)
            if row["type"] in ['A', 'AAAA']:
                if self.current_domain.domain not in row["key"]:
                    rev_key = '%s.%s' % (row["key"], self.current_domain.domain)
                else:
                    rev_key = row["key"]
                self.delete_reverse(row["value"], rev_key)
        self.update_serial = True

    def complete_delete(self, text, line, beginidx, endidx):
        records = self.current_domain.records()
        l = len(line.split())
        if l == 1 or (l == 2 and text):
            if text:
                full_text = line.split()[-1]
            else:
                full_text = text
            diff = len(full_text) - len(text)
            if full_text == '@':
                return [self.current_domain.domain]
            record_set = [y['key'][diff:] for y in records if y['key'].startswith(full_text)]
            # Make list unique
            return list(set(record_set))
        elif l == 2 or (l == 3 and text):
            key = line.split()[1].strip()
            #print("B: %s" % key)
            types = []
            for y in records:
                #print("%s %s %s" % (y['key'], y['key'] == key, key))
                if y['key'] == key:
                    #print("%s type: %s value: %s" % (y['key'], y['type'], y['value']))
                    if y['type'] not in types:
                        types.append(y['type'])
            return [x for x in types if x.startswith(text)]
        elif l == 3 or (l == 4 and text):
            key, key_type = line.split()[1:3]
            if text:
                full_text = line.split()[-1]
            else:
                full_text = text
            diff = len(full_text) - len(text)
            return list(set([x['value'][diff:] for x in records if x['key'] == key and x['type'] == key_type and x['value'].startswith(text)]))
        return []

    def do_deletedomain(self, line):
        if self.current_domain:
            print("Get out of domain context first")
            return False
        if self.todoqueue:
            print("Commit or revert first")
            return False
        d = Domain(line)
        if not d.exists():
            print("Domain %s does not exist" % domain)
            return False
        d.to_delete = True
        self.todoqueue.append(d)

    def complete_deletedomain(self, *args, **kwargs):
        return self.complete_domain(*args, **kwargs)

    def do_EOF(self, line):
        if self.todoqueue:
            print("Revert first")
            return False
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


    def do_show(self, line):
        """Show changes to do
        """
        if not self.todoqueue:
            print("Nothing changed, did you mean list?")
            return
        for thing in self.todoqueue:
            print(thing.show())

    def do_list(self, line):
        """list [filter]
        List Domains/records
        """
        keys = ["key", "type", "value"]
        if line:
            keywords = line.strip().split()
        else:
            keywords = []
        if self.current_domain:
            print("\033[1m{0:<40} {1:<6} {2:<5} {3:>4} {4}\033[0m".format("key", "ttl", "type", "priority", "value"))
            for row in self.current_domain.records():
                if keywords:
                    found = False
                    for x in keys:
                        for k in keywords:
                            if k in row[x]:
                                found = True
                                continue
                    if not found:
                        continue
                print("{key:<40} {ttl:<6} {type:<5} {priority:>4} {value}".format(**row))
        else:
            print("\033[1m{0:<40} {1:<10} {2:>12}\033[0m".format("name", "type", "notified serial"))
            for row in self.get_domains():
                print("{0:<40} {1:<10} {2:>12}".format(*row))
        print("")

    def do_ls(self, line):
        return self.do_list(line)

    def do_toggle_debug(self, line):
        global DEBUG
        DEBUG = not DEBUG

    def get_domains(self):
        db.execute("SELECT name, type, notified_serial FROM domains ORDER BY name")
        return [[a, b, '%s' % c] for a,b,c in db.fetchall()]

if __name__ == '__main__':
    DNSCommander().cmdloop()
