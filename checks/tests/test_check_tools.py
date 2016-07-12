#!/usr/bin/env python

import os, sys, time
import datetime
from datetime import datetime as dtdt
sys.path.insert(0, '../')
import pytest
import check_tools as mod


class Test_valid_iso8601_basic(object):

    def test_valid_short(self):
        assert mod.valid_iso8601('20160102T030405', 'basic') is True

    def test_valid_long(self):
        assert mod.valid_iso8601('20160102T030405123456', 'basic') is True

    def test_invalid_short(self):
        assert mod.valid_iso8601('20160102 030405', 'basic') is False

    def test_invalid_long(self):
        assert mod.valid_iso8601('20160102 030405123456', 'basic') is False

    def test_invalid_misc(self):
        assert mod.valid_iso8601('20160102 030405', 'basic') is False
        assert mod.valid_iso8601(None, 'basic') is False


class Test_valid_iso8601_ext(object):

    def test_valid_short(self):
        assert mod.valid_iso8601('2016-01-02T03:04:05', 'ext') is True

    def test_valid_long(self):
        assert mod.valid_iso8601('2016-01-02T03:04:05.123456', 'ext') is True

    def test_invalid_short(self):
        assert mod.valid_iso8601('2016:01:02T03:04:05', 'ext') is False

    def test_invalid_long(self):
        assert mod.valid_iso8601('2016-01-02T03-04-05-123456', 'ext') is False

    def test_invalid_misc(self):
        assert mod.valid_iso8601('20160102T030405', 'ext') is False
        assert mod.valid_iso8601(None, 'ext') is False


class Test_get_next_dt(object):
    def test_basics(self):
        last_start_dt = datetime.datetime(2015, 01, 02, 0, 0, 0)
        last_stop_dt = datetime.datetime(2015, 01, 02, 23, 59, 59)
        assert mod.get_next_dt(last_start_dt, last_stop_dt)  \
                == (datetime.datetime(2015, 01, 03, 0, 0, 0), datetime.datetime(2015, 1, 3, 23, 59, 59))
    def test_input_is_none(self):
        last_dt = None
        assert mod.get_next_dt(None, None)  == (None, None)


class Test_iso8601_to_dt_basic(object):

    def test_valid_short(self):
        assert mod.iso8601_to_dt('20160102T030405', 'basic') == datetime.datetime(2016, 1, 2, 3, 4, 5)

    def test_valid_long(self):
        assert mod.iso8601_to_dt('20160102T030405123456', 'basic') == datetime.datetime(2016, 1,2,3,4,5, 123456)

    def test_invalid_short(self):
        with pytest.raises(ValueError):
            mod.iso8601_to_dt('20160102 030405', 'basic')

    def test_invalid_long(self):
        with pytest.raises(ValueError):
            mod.iso8601_to_dt('20160102 030405123456', 'basic')

    def test_none(self):
        assert mod.iso8601_to_dt(None, 'basic') is None

    def test_invalid_misc(self):
        with pytest.raises(ValueError):
            mod.iso8601_to_dt('20160102 030405', 'basic')


class Test_iso8601_to_ext(object):

    def test_valid_short(self):
        assert mod.iso8601_to_dt('2016-01-02T03:04:05', 'ext') == datetime.datetime(2016, 1,2,3,4,5)

    def test_valid_long(self):
        assert mod.iso8601_to_dt('2016-01-02T03:04:05.123456', 'ext') == datetime.datetime(2016,1,2,3,4,5,123456)

    def test_invalid_short(self):
        with pytest.raises(ValueError):
            mod.iso8601_to_dt('2016:01:02T03:04:05', 'ext')

    def test_invalid_long(self):
        with pytest.raises(ValueError):
            mod.iso8601_to_dt('2016-01-02T03-04-05-123456', 'ext')

    def test_none(self):
        assert mod.iso8601_to_dt(None, 'ext') is None

    def test_invalid_misc(self):
        with pytest.raises(ValueError):
            mod.iso8601_to_dt('20160102T030405', 'ext')


class Test_dt_to_iso8601(object):

    def test_basic_second(self):
        assert mod.dt_to_iso8601(dtdt(2016, 1, 2, 3, 4, 5), 'basic', 'second') == '20160102T030405'

    def test_basic_ms(self):
        assert mod.dt_to_iso8601(dtdt(2016, 1, 2, 3, 4, 5, 123456), 'basic', 'ms') == '20160102T030405123456'

    def test_none(self):
        with pytest.raises(ValueError):
            mod.dt_to_iso8601(None, 'basic', 'ms')

    def test_ext_second(self):
        assert mod.dt_to_iso8601(dtdt(2016, 1, 2, 3, 4, 5), 'ext', 'second') == '2016-01-02T03:04:05'

    def test_ext_ms(self):
        assert mod.dt_to_iso8601(dtdt(2016, 1, 2, 3, 4, 5, 123456), 'ext', 'ms') == '2016-01-02T03:04:05.123456'

    def test_invalid_args(self):
        with pytest.raises(ValueError):
            mod.dt_to_iso8601(dtdt(2016, 1, 2, 3, 4, 5, 123456), 'foo', 'ms')
        with pytest.raises(ValueError):
            mod.dt_to_iso8601(dtdt(2016, 1, 2, 3, 4, 5, 123456), 'ext', 'foo')
        with pytest.raises(ValueError):
            mod.dt_to_iso8601('foo', 'ext', 'ms')


class Test_get_ymd_filter(object):

    def test_iso8601_basic(self):
        result = mod.get_ymd_filter('20160102T000000', '20160102T235959')
        assert '2016' in result

    def test_iso8601_ext(self):
        result = mod.get_ymd_filter('2016-01-02T03:04:05', '20160102T235959')
        assert '2016' in result

    def test_none_input(self):
        assert mod.get_ymd_filter(None, '20160102T235959') == ''

    def test_bad_dates(self):
        with pytest.raises(ValueError):
            mod.get_ymd_filter('foo', '20160102T235959')



class Test_part_to_dt(object):
    def test_basics(self):
        assert mod.part_to_dt(2015, 01, 02)  == datetime.datetime(2015, 01, 02)

class Test_dt_to_part(object):
    def test_basics(self):
        last_dt = datetime.datetime(2015, 01, 02)
        assert mod.dt_to_parts(last_dt) == (2015, 1, 2)
    def test_input_is_none(self):
        assert mod.dt_to_parts(None) == (None, None, None)
