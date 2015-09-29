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
    cmd  = get_cmd(args.inst, args.db, args.child_table, args.child_col, args.parent_table, args.parent_col)

    r = envoy.run(cmd)
    results = {}
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
    else:
        results['violation_cnt'] = r.std_out.strip()

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
    args = parser.parse_args()

    args.inst         = args.inst         or os.environ.get('hapinsp_instance', None)
    args.db           = args.db           or os.environ.get('hapinsp_database', None)
    args.child_table  = args.child_table  or os.environ.get('hapinsp_table', None)
    args.child_col    = args.child_col    or os.environ.get('hapinsp_checkcustom_child_col', None)
    args.parent_table = args.parent_table or os.environ.get('hapinsp_checkcustom_parent_table', None)
    args.parent_col   = args.parent_col   or os.environ.get('hapinsp_checkcustom_parent_col', None)

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


def get_cmd(inst, db, child_table, child_col, parent_table, parent_col):

    def despacer(val):
        while True:
            old_val = val
            val     = val.replace('  ', ' ')
            if val == old_val:
                break
        return val

    child_tabcol  = '%s.%s' % (child_table, child_col)
    parent_tabcol = '%s.%s' % (parent_table, parent_col)

    sql =   """ WITH t1 AS (                         \
                    SELECT  {c_tabcol},              \
                            {p_tabcol}               \
                    FROM {c_tab}                     \
                        LEFT OUTER JOIN {p_tab}      \
                           ON {c_tabcol} = {p_tabcol}\
                )                                    \
                SELECT COUNT(*)                      \
                FROM t1                              \
                WHERE {p_col} IS NULL                \
            """.format(c_tabcol=child_tabcol, p_tabcol=parent_tabcol, c_tab=child_table, p_tab=parent_table, p_col=parent_col)

    smaller_sql = despacer(sql)
    cmd = """ impala-shell -i %s -d %s --quiet -B -q "%s"
          """ % (inst, db, smaller_sql)
    return cmd


def abort(msg):
    print(msg)
    sys.exit(1)


if __name__ == '__main__':
   sys.exit(main())
