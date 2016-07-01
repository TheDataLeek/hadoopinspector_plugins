#!/usr/bin/env python
import os, sys
import argparse
from os.path import dirname
from pprint import pprint as pp
import envoy

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(os.path.abspath(__file__)))
import hapinsp_formatter

def main():
    args = get_args()
    cmd  = get_cmd(args.inst, args.db, args.table, args.cols, args.ssl)

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
    parser = argparse.ArgumentParser(description="tests table's pk")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--table")
    parser.add_argument("--cols")
    parser.add_argument("--ssl", action='store_true')
    parser.add_argument("--no-ssl", action='store_false', dest='ssl')
    args = parser.parse_args()

    args.inst  = args.inst  or os.environ.get('hapinsp_instance', None)
    args.db    = args.db    or os.environ.get('hapinsp_database', None)
    args.table = args.table or os.environ.get('hapinsp_table', None)
    args.cols  = args.cols  or os.environ.get('hapinsp_checkcustom_cols', None)
    args.ssl   = args.ssl   or os.environ.get('hapinsp_ssl', None)
    if not args.inst:
        abort("Error: instance not provided as arg or env var")
    if not args.db:
        abort("Error: database not provided as arg or env var")
    if not args.table:
        abort("Error: table not provided as arg or env var")
    if not args.cols:
        abort("Error: cols not provided as arg or env var")
    if not args.ssl:
        abort("Error: ssl not provided as arg or env var")

    return args


def get_cmd(inst, db, table, cols, ssl):

    sql =   """ WITH t1 AS (                         \
                    SELECT  %s    ,                  \
                            COUNT(*) AS dup_cnt      \
                    FROM %s                          \
                    GROUP BY %s                      \
                    HAVING COUNT(*) > 1              \
                )                                    \
                SELECT COUNT(*)                      \
                FROM t1                              \
                WHERE dup_cnt > 1                    \
            """ % (cols, table, cols)
    sql = ' '.join(sql.split())
    sslopt = '--ssl' if ssl else ''
    cmd = """ impala-shell -i %s -d %s --quiet -B %s -q "%s"
          """ % (inst, db, sslopt, sql)
    return cmd


def abort(msg):
    print(msg)
    sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
