#!/usr/bin/env python
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""
import os, sys, time
import datetime
import argparse
from os.path import dirname
from pprint import pprint as pp
import envoy
import subprocess

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(os.path.abspath(__file__)))
import hapinsp_formatter

def main():
    args = get_args()
    if args.hapinsp_tablecustom_day_prior is None:
        next_dt = get_first_dt(args.inst, args.db, args.table)
        next_year, next_month, next_day = dt_to_parts(next_dt)
        next_day_has_data = True
    else:
        last_dt = part_to_dt(args.hapinsp_tablecustom_year_prior,
                             args.hapinsp_tablecustom_month_prior,
                             args.hapinsp_tablecustom_day_prior )
        next_dt = get_next_dt(last_dt)
        next_year, next_month, next_day = dt_to_parts(next_dt)
        next_day_has_data = does_part_have_data(args.inst, args.db, args.table, next_year, next_month, next_day)

    results = {}
    if next_day_has_data:
        results['table_status']              = 'active'
        results['mode']                      = 'incremental'
        results['hapinsp_tablecustom_year']  = str(next_year)
        results['hapinsp_tablecustom_month'] = str(next_month)
        results['hapinsp_tablecustom_day']   = str(next_day)
    else:
        results['table_status']              = 'inactive'
        results['mode']                      = 'incremental'
        results['hapinsp_tablecustom_year']  = ''
        results['hapinsp_tablecustom_month'] = ''
        results['hapinsp_tablecustom_day']   = ''

    internal_status_cd = 0
    results['rc'] = max(0, internal_status_cd)
    print(hapinsp_formatter.transform_args(results))

def part_to_dt(year, month, day):
    return datetime.datetime(int(year), int(month), int(day))

def get_next_dt(last_dt):
    if last_dt is None:
        new_dt = None
    else:
        new_dt = last_dt + datetime.timedelta(days=1)
    return new_dt

def dt_to_parts(dt):
    if dt is None:
        return None, None, None
    else:
        return dt.year, dt.month, dt.day

def does_part_have_data(inst, db, table, year, month, day):
    def despacer(val):
        while True:
            old_val = val
            val     = val.replace('  ', ' ')
            if val == old_val:
                break
        return val

    sql =   """ SELECT 'found-data'
                FROM {tab}
                WHERE year={year} AND month={month} AND day={day}
                LIMIT 1
            """.format(tab=table, year=year, month=month, day=day)
    smaller_sql = despacer(sql)
    cmd = """ impala-shell -i %s -d %s --quiet -B -q "%s" | columns | cut -f 1
          """ % (inst, db, smaller_sql)

    r = envoy.run(cmd)
    results = {}
    internal_status_code = -1
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
    else:
        if 'found-data' in r.std_out.strip():
            return True
    return False


def get_first_dt(inst, db, table):
    def despacer(val):
        while True:
            old_val = val
            val     = val.replace('  ', ' ')
            if val == old_val:
                break
        return val

    #sql =   """ impala-shell -i had-data-001 -d rumprod --quiet --output_delimiter ',' -B -q 'show partitions {tab}' 2>/dev/null | head -n 1 """.format(tab=table)
    sql =   """ impala-shell -i had-data-001 -d rumprod --quiet --output_delimiter ',' -B -q 'show partitions {tab}' | head -n 10 """.format(tab=table)
    print('----------------')
    print(sql)
    print('----------------')

    #cmd1 = ['impala-shell', '-i', 'had-data-001', '-d', 'rumprod',
    #        '--quiet', '--output_delimiter', ',', '-B',
    #       '-q', "show partitions fact_dns_v2"]
    #cmd2 = ['head', '-n', '1']
    #ps = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    #stdout = subprocess.check_output(cmd2, stdin=ps.stdout)

    stdout = subprocess.check_output(sql, shell=True)[:-1] # remove ending newline

    #print("====================")
    #p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #p2 = subprocess.Popen(cmd2, stdin=p1.stdout)
    #stdout, stderr = p2.communicate()
    #print("stdout: ")
    #pp(stdout)
    #print("stderr: ")
    #pp(stderr)
    #print("done")
    #print("====================")

    #if p2.returncode != 0:
    #    print("First_dt failed1")
    #    raise ValueError

    print("stdout: ")
    print(stdout)
    fields = stdout.split(',')
    print(fields)
    assert len(fields) == 10, "Invalid fields: %s" % ','.join(fields)
    first_dt = datetime.datetime(int(fields[0]), int(fields[1]), int(fields[2]))
    return first_dt


def get_args():
    parser = argparse.ArgumentParser(description="determines next partition")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--table")
    parser.add_argument("--mode")
    args = parser.parse_args()

    args.inst  = args.inst  or os.environ.get('hapinsp_instance', None)
    args.db    = args.db    or os.environ.get('hapinsp_database', None)
    args.table = args.table or os.environ.get('hapinsp_table', None)
    args.mode  = args.mode  or os.environ.get('hapinsp_check_mode', None)
    args.rc_prior                        = os.environ.get('hapinsp_tablecustom_internal_rc_prior', None)
    args.hapinsp_tablecustom_year_prior  = os.environ.get('hapinsp_tablecustom_year_prior', None)
    args.hapinsp_tablecustom_month_prior = os.environ.get('hapinsp_tablecustom_month_prior', None)
    args.hapinsp_tablecustom_day_prior   = os.environ.get('hapinsp_tablecustom_day_prior', None)
    if not args.inst:
        abort("Error: instance not provided as arg or env var")
    if not args.db:
        abort("Error: database not provided as arg or env var")
    if not args.table:
        abort("Error: table not provided as arg or env var")
    if args.mode not in ('incremental', 'full', 'auto'):
        abort("Error: mode value is invalid: %s" % args.mode)
    return args


def abort(msg):
    print(msg)
    sys.exit(1)

def isnumeric(val):
    try:
        int(val)
    except TypeError:
        return False
    except ValueError:
        return False
    else:
        return True

if __name__ == '__main__':
   sys.exit(main())
