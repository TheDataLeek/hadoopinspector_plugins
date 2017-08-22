#!/usr/bin/env python3
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
    cmd  = get_cmd(args.inst, args.db, args.table)

    r = envoy.run(cmd)
    results = {}
    internal_status_code = -1
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
    else:
        violations = r.std_out.strip()
        if not isnumeric(violations):
            internal_status_code = 1
            fixed_violations = 1
        else:
            if int(violations) == -1:
                fixed_violations = 1
            else:
                fixed_violations = 0

        results['violation_cnt'] = fixed_violations

    results['rc'] = max(r.status_code, internal_status_code)
    print(hapinsp_formatter.transform_args(results))


def get_args():
    parser = argparse.ArgumentParser(description="tests table statistics")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--table")
    args = parser.parse_args()

    args.inst  = args.inst  or os.environ.get('hapinsp_instance', None)
    args.db    = args.db    or os.environ.get('hapinsp_database', None)
    args.table = args.table or os.environ.get('hapinsp_table', None)
    if not args.inst:
        abort("Error: instance not provided as arg or env var")
    if not args.db:
        abort("Error: database not provided as arg or env var")
    if not args.table:
        abort("Error: table not provided as arg or env var")
    return args


def get_cmd(inst, db, table):

    sql = """ SHOW TABLE STATS {tab}   \
          """.format(tab=table)
    sql = ' '.join(sql.split())
    cmd = """ impala-shell -i %s -d %s --quiet -B --ssl -q "%s" | columns | cut -f 1
          """ % (inst, db, sql)
    return cmd


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
