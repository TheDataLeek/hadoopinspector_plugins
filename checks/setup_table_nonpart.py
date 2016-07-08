#!/usr/bin/env python
""" Identifies data start & stop times for tables that are not partitioned.

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
import check_tools as tools

INTERNAL_STATUS_CD = 0  #FIXME: should probably set various codes, just aborting on any issues for now

def main():
    next_start_dt       = None
    next_stop_dt        = None
    next_period_has_data = False

    args = get_args()
    validate_args(args)

    if args.mode == 'incremental':
        if args.data_start_ts_prior == '':
            first_dt = get_first_dt_by_ymd(args.inst, args.db, args.table, args.ssl)
            if first_dt:
                next_period_has_data = True
                next_start_dt       = tools.dt_override(first_dt, hour=0, minute=0, second=0, ms=0)
                next_stop_dt        = tools.dt_override(first_dt, hour=23, minute=59, second=59, ms=999999)
        else:
            next_start_dt, next_stop_dt = tools.get_next_dt(tools.iso8601_to_dt(args.data_start_ts_prior),
                                                            tools.iso8601_to_dt(args.data_stop_ts_prior))
            next_period_has_data = does_period_have_data(args.inst, args.db, args.ssl, args.table, next_start_dt, next_stop_dt)
    else:
        next_period_has_data = does_period_have_data(args.inst, args.db, args.ssl, args.table, start_dt=None, stop_dt=None)

    results = {}
    if next_period_has_data:
        results['table_status']       = 'active'
        results['mode']               = args.mode
        if args.mode == 'incremental':
            results['data_start_ts']  = tools.dt_to_iso8601(next_start_dt, precision='second')
            results['data_stop_ts']   = tools.dt_to_iso8601(next_stop_dt, precision='second')
        else:
            results['data_start_ts']  = ''
            results['data_stop_ts']   = ''
    else:
        results['table_status']       = 'inactive'
        results['mode']               = args.mode
        results['data_start_ts']      = args.data_start_ts_prior
        results['data_stop_ts']       = args.data_stop_ts_prior

    results['rc'] = INTERNAL_STATUS_CD
    results['log'] = 'start_ts_prior: %s, stop_ts_prior: %s, next_period_has_data: %s'\
            % (args.data_start_ts_prior, args.data_stop_ts_prior, next_period_has_data)
    for (key, val) in os.environ.items():
        if 'hapinsp' in key:
            results['log'] += ', %s:%s' % (key, val)
    print(hapinsp_formatter.transform_args(results))



def does_period_have_data(inst, db, ssl, table, start_dt, stop_dt):
    assert start_dt is None or isinstance(start_dt, datetime.datetime)
    assert stop_dt is None or isinstance(stop_dt, datetime.datetime)
    if start_dt is None or stop_dt is None:
        ymd_filter = ''
    else:
        ymd_filter = tools.get_ymd_filter(tools.dt_to_iso8601(start_dt), tools.dt_to_iso8601(stop_dt))
    ssl_opt    = tools.format_ssl(ssl)

    sql =   """ SELECT 'found-data'
                FROM {tab} c
                WHERE 1 = 1
                     {filter}
                LIMIT 1
            """.format(tab=table, filter=ymd_filter)
    sql = ' '.join(sql.split())
    cmd = """ impala-shell -i {inst} -d {db} --quiet -B {ssl} -q "{sql}" | columns | cut -f 1
          """.format(inst=inst, db=db, ssl=ssl_opt, sql=sql)
    r = envoy.run(cmd)
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
        tools.abort("Error: does_period_have_data() failed!")
    else:
        if 'found-data' in r.std_out.strip():
            return True
        else:
            return False


def get_first_dt_by_ymd(inst, db, table, ssl):
    sql =   """ with year_tab AS (
                   SELECT MIN(year) AS year
                     FROM {tab}
                ),
                mon_tab AS (
                   SELECT MIN(month) AS month
                     FROM {tab}   t
                        INNER JOIN year_tab yt
                           ON t.year = yt.year
                ),
                day_tab AS (
                   SELECT MIN(day) AS day
                     FROM {tab}   t
                        INNER JOIN year_tab yt
                           ON t.year = yt.year
                        INNER JOIN mon_tab mt
                           ON t.month = mt.month
                )
                SELECT year, month, day
                FROM year_tab
                   CROSS JOIN mon_tab
                   CROSS JOIN day_tab
            """.format(tab=table)

    ssl_option = tools.format_ssl(ssl)
    sql = ' '.join(sql.split())
    cmd =   """ impala-shell -i {inst} -d {db} --quiet --output_delimiter ',' -B {ssl} -q '{sql}' """\
            .format(inst=inst, db=db, sql=sql, ssl=ssl_option)
    try:
        stdout = subprocess.check_output(cmd, shell=True)[:-1] # remove ending newline
    except subprocess.CalledProcessError as e:
        return None #FIXME: why would this happen?

    if stdout:
        fields = stdout.split(',')
        assert len(fields) == 3, "Invalid fields: %s" % ','.join(fields)
        return datetime.datetime(int(fields[0]), int(fields[1]), int(fields[2]))
    else:  # no data found
        return None


def get_args():
    parser = argparse.ArgumentParser(description="determines next partition")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--table")
    parser.add_argument("--ssl")
    parser.add_argument("--mode")
    parser.add_argument("--data-start-ts-prior")
    parser.add_argument("--data-stop-ts-prior")
    parser.add_argument("--rc-prior")
    args = parser.parse_args()

    args.inst  = args.inst  or os.environ.get('hapinsp_instance', None)
    args.db    = args.db    or os.environ.get('hapinsp_database', None)
    args.table = args.table or os.environ.get('hapinsp_table', None)
    args.ssl   = args.ssl   or os.environ.get('hapinsp_ssl', None)
    args.mode  = args.mode  or os.environ.get('hapinsp_check_mode', None)
    args.data_start_ts_prior = args.data_start_ts_prior or os.environ.get('hapinsp_table_data_start_ts_prior', '')
    args.data_stop_ts_prior  = args.data_stop_ts_prior or os.environ.get('hapinsp_table_data_stop_ts_prior', '')
    args.rc_prior            = args.rc_prior or os.environ.get('hapinsp_tablecustom_internal_rc_prior', None)

    return args


def validate_args(args):
    if not args.inst:
        tools.abort("Error: instance not provided as arg or env var")
    if not args.db:
        tools.abort("Error: database not provided as arg or env var")
    if not args.table:
        tools.abort("Error: table not provided as arg or env var")
    if args.mode not in ('incremental', 'full', 'auto'):
        tools.abort("Error: mode value is invalid: %s" % args.mode)

    if args.data_start_ts_prior and not tools.valid_iso8601(args.data_start_ts_prior, 'basic'):
        tools.abort("Error: invalid timestamp: %s" % args.data_start_ts_prior)
    if args.data_stop_ts_prior and not tools.valid_iso8601(args.data_stop_ts_prior, 'basic'):
        tools.abort("Error: invalid timestamp: %s" % args.data_stop_ts_prior)

    return args


if __name__ == '__main__':
   sys.exit(main())
