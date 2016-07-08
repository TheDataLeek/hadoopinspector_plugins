#!/usr/bin/env python
""" Identifies data start & stop times for tables that are not partitioned.

"""
import os, sys, time
import datetime
from datetime import datetime as dtdt
import argparse
from os.path import dirname
from pprint import pprint as pp
import envoy
import subprocess


#---------------------------------------------------------------------------
# iso8601 and datetime functions
#---------------------------------------------------------------------------

def get_next_dt(last_start_dt, last_stop_dt):
    if last_start_dt is None or last_stop_dt is None:
        return None, None
    elif not isinstance(last_start_dt, datetime.datetime):
        raise ValueError('last_start_dt is not a datetime')
    elif not isinstance(last_stop_dt, datetime.datetime):
        raise ValueError('last_stop_dt is not a datetime')
    else:
        interval = last_stop_dt - last_start_dt
        new_start_dt = last_start_dt + interval + datetime.timedelta(seconds=1)
        new_stop_dt = last_stop_dt + interval + datetime.timedelta(seconds=1)
        return new_start_dt, new_stop_dt


def valid_iso8601(iso8601, isotype):
    """ Verifies that input iso8601-format is valid
    Args:
        iso8601 (str): must have one of the following formats:
            yyyy-mm-ddThh:mm:ss
            yyyy-mm-ddThh:mm:ss.mmmmmm
            yyyymmddThhmmss
            yyyymmddThhmmssmmmmmm
        isotype (str): defaults to 'any', but can have any of the following formats:
            'basic'
            'ext'
            'any'
    Returns:
        dt (datetime.datetime): or None if iso8601 is None
    """

    assert isotype in ('basic', 'ext', 'any')
    if iso8601 is None:
        return False
    if isotype == 'any':
        isotype = 'ext' if len(iso8601) in (19, 26) else 'basic'

    try:
        if isotype == 'ext':
            if len(iso8601) == 19:
                datetime.datetime.strptime(iso8601, "%Y-%m-%dT%H:%M:%S")
            elif len(iso8601) == 26:
                datetime.datetime.strptime(iso8601, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                return False
        else:
            if len(iso8601) == 15:
                datetime.datetime.strptime(iso8601, "%Y%m%dT%H%M%S")
            elif len(iso8601) == 21:
                datetime.datetime.strptime(iso8601, "%Y%m%dT%H%M%S%f")
            else:
                return False
    except (ValueError, TypeError):
        return False
    else:
        return True


def iso8601_to_dt(iso8601, isotype='any'):
    """ Returns input iso8601-format timestamp in datetime.datetime format.
    Args:
        iso8601 (str): must have one of the following formats:
            yyyy-mm-ddThh:mm:ss
            yyyy-mm-ddThh:mm:ss.mmmmmm
            yyyymmddThhmmss
            yyyymmddThhmmssmmmmmm
        isotype (str): defaults to 'any', but can have any of the following formats:
            'basic'
            'ext'
            'any'
    Returns:
        dt (datetime.datetime): or None if iso8601 is None
    """
    assert isotype in ('basic', 'ext', 'any')
    if iso8601 is None:
        return None
    if isotype == 'any':
        isotype = 'ext' if len(iso8601) in (19, 26) else 'basic'
    dt = None
    try:
        if isotype == 'ext':
            if len(iso8601) == 19:
                dt = datetime.datetime.strptime(iso8601, "%Y-%m-%dT%H:%M:%S")
            elif len(iso8601) == 26:
                dt = datetime.datetime.strptime(iso8601, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                raise ValueError('Invalid iso8601 value: %s' % iso8601)
        else:
            if len(iso8601) == 15:
                dt = datetime.datetime.strptime(iso8601, "%Y%m%dT%H%M%S")
            elif len(iso8601) == 21:
                dt = datetime.datetime.strptime(iso8601, "%Y%m%dT%H%M%S%f")
            else:
                raise ValueError('Invalid iso8601 value: %s' % iso8601)
    except (ValueError, TypeError):
        raise ValueError('Invalid iso8601 value: %s' % iso8601)
    else:
        return dt


def dt_to_iso8601(dt, isotype='basic', precision='second'):
    if not isinstance(dt, datetime.datetime):
        raise ValueError('invalid dt arg')
    if isotype not in ('ext', 'basic'):
        raise ValueError('invalid isotype')
    if precision not in ('ms', 'second'):
        raise ValueError('invalid precision')

    iso8601 = dt.isoformat()
    if isotype == 'basic':
        iso8601 = iso8601.replace('-', '').replace(':', '')
    if precision == 'second':
        parts = iso8601.split('.')
        iso8601 = parts[0]
    elif precision == 'ms' and isotype == 'basic':
        parts = iso8601.split('.')
        iso8601 = ''.join(parts)

    assert valid_iso8601(iso8601, isotype)
    return iso8601


def dt_override(dt, hour=None, minute=None, second=None, ms=None):
    if hour is not None:
        dt = dt.replace(hour=hour)
    if minute is not None:
        dt = dt.replace(minute=minute)
    if second is not None:
        dt = dt.replace(second=second)
    if ms is not None:
        dt = dt.replace(microsecond=ms)
    return dt


#---------------------------------------------------------------------------
# timestamp part functions (for separate year, month, day fields)
#---------------------------------------------------------------------------

def get_ymd_filter(start_iso8601, stop_iso8601):
    if not start_iso8601 or not stop_iso8601:
        return ''
    if not valid_iso8601(start_iso8601, 'any'):
        raise ValueError('invalid start_iso8601 timestamp')
    if not valid_iso8601(stop_iso8601, 'any'):
        raise ValueError('invalid stop_iso8601 timestamp')

    start_dt = iso8601_to_dt(start_iso8601, 'any')
    stop_dt  = iso8601_to_dt(stop_iso8601, 'any')

    part_filter = 'AND ( (c.year > {y}) OR (c.year = {y} and c.month > {m}) OR (c.year = {y} AND c.month = {m} and c.day <= {d}) )'\
            .format(y=start_dt.year, m=start_dt.month, d=start_dt.day)
    part_filter += 'AND ( (c.year < {y}) OR (c.year = {y} and c.month < {m}) OR (c.year = {y} AND c.month = {m} and c.day <= {d}) )'\
            .format(y=stop_dt.year, m=stop_dt.month, d=stop_dt.day)
    return part_filter

def part_to_dt(year, month, day):
    return datetime.datetime(int(year), int(month), int(day))

def part_to_start_dt(year, month, day, hour=0, minute=0, second=0):
    return datetime.datetime(int(year), int(month), int(day), hour, minute, second)\
            .isoformat().replace('-', '').replace(':', '')

def part_to_stop_dt(year, month, day, hour=23, minute=59, second=59):
    return datetime.datetime(int(year), int(month), int(day), hour, minute, second)\
            .isoformat().replace('-', '').replace(':', '')

def dt_to_parts(dt):
    if dt is None:
        return None, None, None
    else:
        return dt.year, dt.month, dt.day



#---------------------------------------------------------------------------
# misc functions
#---------------------------------------------------------------------------

def format_ssl(ssl):
    if ssl:
        return '--ssl'
    else:
        return ''


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
