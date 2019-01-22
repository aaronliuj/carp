#!/usr/bin/env python
# -*- coding: utf-8 -*-

import arrow
import datetime
from . import config


FREQ_DAY = 'D'

## not support
FREQ_HOUR = 'H'
FREQ_MIN = 'M'



def __arrow_convert(date):
    if isinstance(date, datetime.date):
        return arrow.get(date)
    elif isinstance(date, arrow.Arrow):
        return date
    elif isinstance(date, str):
        return arrow.get(date)
    else:
        raise RuntimeError("unknown type")

def __datetime_convert(date):
    if isinstance(date, datetime.date):
        return datetime.datetime.combine(date, datetime.datetime.min.time())
    else:
        raise NotImplementedError('not implement {}'.format(type(date)))


## support 'D' 'H' 'M'
## FIXME freq isn't support
def today(freq = 'D'):
    now = arrow.now()
    if freq == 'D':
        return now.datetime
    elif freq == 'H':
        return now.replace(minute = 0, second = 0, microsecond = 0).datetime
    elif freq == 'M':
        return now.replace(second = 0, microsecond= 0).datetime




def compare(c1, c2, freq = 'D'):
    c1 = __datetime_convert(c1)
    c2 = __datetime_convert(c2)
    if freq == 'D':
        return (c1 - c2).days
    elif freq == 'H':
        d = (c1.date() - c2.date()).days
        return int((c1 - c2).seconds/3600 + d * 24)
    elif freq == 'M':
        d = (c1.date() - c2.date()).days
        return int((c1 - c2).seconds/60 + d * 24 * 60)
    else:
        raise NotImplementedError('not implement')

def next(d, freq = 'D'):
    return shift(d, 1, freq = freq)


def shift(d, n, freq = 'D'):
    d = __datetime_convert(d)
    if freq == 'D':
        return d + datetime.timedelta(days = n)
    else:
        raise NotImplementedError('not implement')


def format(s, str = 'YYYYMMDD'):
    s = __arrow_convert(s)
    return s.format(str)



def pdTimestamp2Datetime(l):
    return list(map(lambda x: x.to_pydatetime(), l) if l is not None else None)



if __name__ == "__main__":
    config.logger.debug(today('H'))
    config.logger.debug(format(datetime.datetime.now()))
    config.logger.debug(compare(datetime.datetime.now(), datetime.datetime(2018, 3, 1), freq = 'M'))
    config.logger.debug(compare(datetime.datetime.now(), datetime.datetime.now()))
    config.logger.debug(shift(datetime.date(2018, 3, 1), 1))

