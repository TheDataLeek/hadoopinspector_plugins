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
    enums = args.enums.split(',')
    cmd  = get_cmd(args.inst, args.db, args.table, args.col, enums)

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
    parser = argparse.ArgumentParser(description="checks that column contents match enum list")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--table")
    parser.add_argument("--col")
    parser.add_argument("--enums")
    args = parser.parse_args()

    args.inst  = args.inst    or os.environ.get('hapinsp_instance', None)
    args.db    = args.db      or os.environ.get('hapinsp_database', None)
    args.table = args.table   or os.environ.get('hapinsp_table', None)
    args.col   = args.col     or os.environ.get('hapinsp_checkcustom_col', None)
    args.enums = args.enums   or os.environ.get('hapinsp_checkcustom_enums', None)

    if not args.inst:
        abort("Error: instance not provided as arg or env var")
    if not args.db:
        abort("Error: database not provided as arg or env var")
    if not args.table:
        abort("Error: table not provided as arg or env var")
    if not args.col:
        abort("Error: col not provided as arg or env var")
    if not args.enums:
        abort("Error: enums not provided as arg or env var")

    return args


def get_cmd(inst, db, table, col, enums):

    enums_string = ','.join(enums)
    sql =   """ SELECT COUNT(*) \
                  FROM {tab} \
                 WHERE {col} NOT IN ({enums})    \
            """.format(tab=table, col=col, enums=enums_string)

    sql = ' '.join(sql.split())
    cmd = """ impala-shell -i %s -d %s --quiet -B --ssl -q "%s"
          """ % (inst, db, sql)
    return cmd


def abort(msg):
    print(msg)
    sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
