#!/usr/bin/env python
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""
import os, sys, time
import argparse
from os.path import dirname
from pprint import pprint as pp
import envoy

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(os.path.abspath(__file__)))
import hapinsp_formatter

def main():
    args = get_args()
    cmd, mode  = get_cmd(args.inst, args.db, args.child_table, args.child_col, args.parent_table, args.parent_col,
                   args.year, args.month, args.day)

    r = envoy.run(cmd)
    results = {}
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
    else:
        results['violation_cnt'] = r.std_out.strip()
        results['mode'] = mode

    results['rc'] = r.status_code
    print(hapinsp_formatter.transform_args(results))


def get_args():
    parser = argparse.ArgumentParser(description="tests dimension's foreign key")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--child-table")
    parser.add_argument("--child-col")
    parser.add_argument("--parent-table")
    parser.add_argument("--parent-col")
    parser.add_argument("--year")
    parser.add_argument("--month")
    parser.add_argument("--day")
    args = parser.parse_args()

    args.inst         = args.inst         or os.environ.get('hapinsp_instance', None)
    args.db           = args.db           or os.environ.get('hapinsp_database', None)
    args.child_table  = args.child_table  or os.environ.get('hapinsp_table', None)
    args.child_col    = args.child_col    or os.environ.get('hapinsp_checkcustom_child_col', None)
    args.parent_table = args.parent_table or os.environ.get('hapinsp_checkcustom_parent_table', None)
    args.parent_col   = args.parent_col   or os.environ.get('hapinsp_checkcustom_parent_col', None)
    args.year         = args.year         or os.environ.get('hapinsp_tablecustom_year', None)
    args.month        = args.month        or os.environ.get('hapinsp_tablecustom_month', None)
    args.day          = args.day          or os.environ.get('hapinsp_tablecustom_day', None)
    args.table_mode   = os.environ.get('hapinsp_table_mode', None)
    args.check_mode   = os.environ.get('hapinsp_check_mode', None)

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

    return args


def get_cmd(inst, db, child_table, child_col, parent_table, parent_col, year, month, day):

    def despacer(val):
        while True:
            old_val = val
            val     = val.replace('  ', ' ')
            if val == old_val:
                break
        return val

    child_tabcol  = '%s.%s' % (child_table, child_col)
    parent_tabcol = '%s.%s' % (parent_table, parent_col)
    if day:
        part_filter = 'AND year={} AND month={} and day={}'.format(year, month, day)
    else:
        part_filter = ''

    sql =   """ WITH t1 AS (                         \
                    SELECT  {c_tabcol} AS child_col, \
                            {p_tabcol} AS par_col    \
                    FROM {c_tab}                     \
                        LEFT OUTER JOIN {p_tab}      \
                           ON {c_tabcol} = {p_tabcol}\
                           {p_filter}                \
                    WHERE {p_tabcol} IS NULL         \
                )                                    \
                SELECT COALESCE(COUNT(*), 0)         \
                FROM t1                              \
            """.format(c_tabcol=child_tabcol, p_tabcol=parent_tabcol, c_tab=child_table,
                       p_tab=parent_table, p_col=parent_col, p_filter=part_filter)

    smaller_sql = despacer(sql)
    cmd = """ impala-shell -i %s -d %s --quiet -B -q "%s"
          """ % (inst, db, smaller_sql)
    mode = 'incremental' if day else 'full'
    return cmd, mode


def abort(msg):
    print(msg)
    sys.exit(1)


if __name__ == '__main__':
   sys.exit(main())
