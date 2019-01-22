#!/usr/bin/env python
# -*- coding: utf-8 -*-
from . import dataquery
from . import dateutils

def get_stock_kline(code, start, end, freq):
    query = dataquery.DataQuery()
    df = query.stock_kline(code, freq, start, end)
    x = dateutils.pdTimestamp2Datetime(df['trade_date'])
    return df


def sync_all():
    pass


if __name__ == "__main__":
    print(get_stock_kline('000002.SZ', '19961111', '19971111', freq = 'D'))
