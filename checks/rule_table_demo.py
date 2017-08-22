#!/usr/bin/env python3
import os, sys, time
import argparse
from os.path import dirname
from pprint import pprint as pp
import envoy

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
sys.path.insert(0, dirname(os.path.abspath(__file__)))
import hapinsp_formatter

def main():
    results = {}
    results['violation_cnt'] = '4'
    results['rc'] = '0'
    print(hapinsp_formatter.transform_args(results))


if __name__ == '__main__':
   sys.exit(main())
