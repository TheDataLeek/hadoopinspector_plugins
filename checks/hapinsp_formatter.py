#!/usr/bin/env python3
import os, sys, json

def transform_args(args):
    args_dict = dict(args)
    results_dict = {}

    for key in args_dict:
        if key == 'rc':
            results_dict[key] = args_dict[key]
        elif key == 'table_status':
            results_dict[key] = args_dict[key]
        elif key == 'data_start_ts':
            results_dict['data_start_ts'] = args_dict[key]
        elif key == 'data_stop_ts':
            results_dict['data_stop_ts'] = args_dict[key]
        elif key == 'violation_cnt':
            results_dict['violations'] = args_dict[key]
        elif key == 'mode':
            results_dict['mode'] = args_dict[key]
        elif key == 'log':
            results_dict['log'] = args_dict[key]
        elif key.startswith('hapinsp_tablecustom_'):
            results_dict[key] = args_dict[key]
        elif key.startswith('hapinsp_'):
            results_dict[key] = args_dict[key]
        else:
            raise ValueError("Invalid arg: %s" % key)
    return json.dumps(results_dict)

