#!/usr/bin/env python
"""
This source code is protected by the BSD license.  See the file "LICENSE"
in the source code root directory for the full language or refer to it here:
   http://opensource.org/licenses/BSD-3-Clause
Copyright 2015 Will Farmer and Ken Farmer
"""
import os, sys, json

def transform_args(args):
    args_dict = dict(args)
    results_dict = {}
      
    for key in args_dict:
        if key == 'rc':
            results_dict[key] = args_dict[key]
        elif key == 'table_status':
            results_dict[key] = args_dict[key]
        elif key == 'violation_cnt':
            results_dict['violations'] = args_dict[key]
        elif key == 'mode':
            results_dict['mode'] = args_dict[key]
        elif key.startswith('hapinsp_tablecustom_'):
            results_dict[key] = args_dict[key]
        else:
            raise ValueError("Invalid arg: %s" % key)
    return json.dumps(results_dict)

