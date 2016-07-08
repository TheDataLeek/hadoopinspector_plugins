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
    cmd, mode  = get_cmd(args.inst, args.db, args.table)

    r = envoy.run(cmd)
    violations = get_violations(r.std_out)
    results = {}
    if r.status_code != 0:
        print(cmd)
        print(r.std_err)
        print(r.std_out)
    else:
        results['violation_cnt'] = str(violations)
        results['mode'] = mode

    results['rc'] = r.status_code
    results['hapinsp_table_mode'] = args.table_mode
    results['hapinsp_check_mode'] = args.check_mode
    print(hapinsp_formatter.transform_args(results))


def get_rowcnt_offset(std_out):
    try:
        for i, val in enumerate(std_out.split('\n')[0].split(',')):
            if val == '#Rows':
                return i
        else:
            raise ValueError('#Rows not found')
    except IndexError:  # probably empty table
        if len(std_out) == 0:
            return -1
        else:
            raise ValueError('Results not parsable')



def get_violations(std_out):
    rowcnt_col = get_rowcnt_offset(std_out)
    if rowcnt_col == -1:
        return -1
    else:
        violations = 0
        for row in std_out.split('\n'):
            fields = row.split(',')
            try:
                if fields[rowcnt_col] == '-1':
                    violations += 1
            except IndexError:
                pass
        return violations




def get_args():
    parser = argparse.ArgumentParser(description="tests whether or not partitioned table's stats exist")
    parser.add_argument("--inst")
    parser.add_argument("--db")
    parser.add_argument("--table")
    args = parser.parse_args()

    args.inst         = args.inst         or os.environ.get('hapinsp_instance', None)
    args.db           = args.db           or os.environ.get('hapinsp_database', None)
    args.table        = args.table        or os.environ.get('hapinsp_table', None)
    args.table_mode   = os.environ.get('hapinsp_table_mode', None)
    args.check_mode   = os.environ.get('hapinsp_check_mode', None)

    if not args.inst:
        abort("Error: instance not provided as arg or env var")
    if not args.db:
        abort("Error: database not provided as arg or env var")
    if not args.table:
        abort("Error: table not provided as arg or env var")

    return args


def get_cmd(inst, db, table):

    sql =   """ SHOW TABLE STATS {tab} \
            """.format(tab=table)
    sql = ' '.join(sql.split())
    cmd = """ impala-shell -i %s -d %s --quiet -B --output_delimiter ',' --print_header --ssl -q "%s"
          """ % (inst, db, sql)
    mode = 'full'
    return cmd, mode


def abort(msg):
    print(msg)
    sys.exit(1)


if __name__ == '__main__':
    sys.exit(main())
