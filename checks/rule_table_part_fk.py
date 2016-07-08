#!/usr/bin/env python
import os, sys, time
import argparse
from os.path import dirname
from pprint import pprint as pp
import envoy

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(os.path.abspath(__file__)))
import hapinsp_formatter
import check_tools as tools

def main():
    args = get_args()
    validate_args(args)
    cmd, mode  = get_cmd(args.inst, args.db, args.child_table, args.child_col, args.parent_table, args.parent_col,
                         args.start_ts, args.stop_ts, args.ssl)
    r = envoy.run(cmd)
    results = {}
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
    else:
        results['violation_cnt'] = r.std_out.strip()
        results['mode'] = mode

    results['log'] = 'start_ts={}, stop_ts={}'.format(args.start_ts, args.stop_ts)
    results['rc'] = r.status_code
    print(hapinsp_formatter.transform_args(results))


def get_args():
    parser = argparse.ArgumentParser(description="tests table's foreign key")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--ssl")
    parser.add_argument("--child-table")
    parser.add_argument("--child-col")
    parser.add_argument("--parent-table")
    parser.add_argument("--parent-col")
    parser.add_argument("--start-ts")
    parser.add_argument("--stop-ts")
    args = parser.parse_args()

    args.inst         = args.inst         or os.environ.get('hapinsp_instance', None)
    args.db           = args.db           or os.environ.get('hapinsp_database', None)
    args.ssl          = args.ssl          or os.environ.get('hapinsp_ssl', None)
    args.child_table  = args.child_table  or os.environ.get('hapinsp_table', None)
    args.child_col    = args.child_col    or os.environ.get('hapinsp_checkcustom_child_col', None)
    args.parent_table = args.parent_table or os.environ.get('hapinsp_checkcustom_parent_table', None)
    args.parent_col   = args.parent_col   or os.environ.get('hapinsp_checkcustom_parent_col', None)
    args.start_ts     = args.start_ts     or os.environ.get('hapinsp_table_data_start_ts', None)
    args.stop_ts      = args.stop_ts      or os.environ.get('hapinsp_table_data_stop_ts', None)
    args.table_mode   = os.environ.get('hapinsp_table_mode', None)
    args.check_mode   = os.environ.get('hapinsp_check_mode', None)

    return args


def validate_args(args):

    if not args.inst:
        abort("Error: instance not provided as arg or env var")
    if not args.db:
        abort("Error: database not provided as arg or env var")
    if not args.child_table:
        abort("Error: child_table not provided as arg or env var")
    if not args.child_col:
        abort("Error: child_col not provided as arg or env var")
    if not args.parent_table:
        abort("Error: parent_table not provided as arg or env var")
    if not args.parent_col:
        abort("Error: parent_col not provided as arg or env var")
    if args.start_ts and not tools.valid_iso8601(args.start_ts, 'basic'):
        abort("Error: start_ts is invalid: %s" % args.start_ts)
    if args.stop_ts and not tools.valid_iso8601(args.stop_ts, 'basic'):
        abort("Error: stop_ts is invalid: %s" % args.stop_ts)



def get_cmd(inst, db, child_table, child_col, parent_table, parent_col, start_ts, stop_ts, ssl):

    if start_ts is None or stop_ts is None:
        filter = ''
    else:
        filter = tools.get_ymd_filter(start_ts, stop_ts)

    sql =   """ WITH t1 AS (                         \
                    SELECT  c.{c_col} AS child_col,  \
                            p.{p_col} AS par_col     \
                    FROM {c_tab} c                   \
                        LEFT OUTER JOIN {p_tab} p    \
                           ON c.{c_col} = p.{p_col}  \
                    WHERE p.{p_col} IS NULL          \
                          {filter}                   \
                )                                    \
                SELECT COALESCE(COUNT(*), 0)         \
                FROM t1                              \
            """.format(c_col=child_col, p_col=parent_col, c_tab=child_table,
                       p_tab=parent_table, filter=filter)

    sql = ' '.join(sql.split())
    ssl_opt = tools.format_ssl(ssl)
    cmd = """ impala-shell -i {inst} -d {db} --quiet -B {ssl} -q "{sql}"
          """.format(inst=inst, db=db, ssl=ssl_opt, sql=sql)
    mode = 'incremental' if filter else 'full'
    return cmd, mode


def abort(msg):
    print(msg)
    sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
