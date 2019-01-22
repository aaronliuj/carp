#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tushare as ts
from . import config
from . import dateutils as du


class TuShareApi:

    __clients = {}

    @classmethod
    def get(cls, token):
        if token not in cls.__clients:
            cls.__clients[token] = TuShareApi(token)
        return cls.__clients[token]

    def __init__(self, token):
        self.__pro = ts.pro_api(token)


    def get_client(self):
        return self.__pro


    def get_all_calendar(self, exchange = '', is_open = 1, start = None, end = None):
        if start is None:
            start = '19900101'
        else:
            start = du.format(start)

        if end is None:
            end = du.format(du.today())
        else:
            end = du.format(end)

        client = self.get_client()
        df = client.trade_cal(exchange= exchange,
        start_date = start,
        end_date= end,
        fields = 'exchange, cal_date,is_open,pretrade_date',
        is_open = is_open)
        return df

    def get_stock_list(self):
        client = self.get_client()
        return client.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')


    def get_bar(self, code, start_date = None, end_date = None, freq = 'D'):
        client = self.get_client()
        if start_date is None:
            start = '19920101'
        else:
            start = du.format(start_date)

        if end_date is None:
            end = du.format(du.today())
        else:
            end = du.format(end_date)
        # 后复权
        return ts.pro_bar(pro_api = self.__pro, ts_code = code, start_date = start,
                end_date = end, adj = 'qfq', freq = freq).reset_index(drop = True)


if __name__ == "__main__":
    api = TuShareApi("fc185d3bbab13076d732ec308facf16124cefc08b5935338e5860559")
    client = api.get_client()
    df = api.get_bar('000002.SZ',  freq = 'D')
    config.logger.debug(df)

    #config.logger.debug(api.get_stock_list())

    #config.logger.debug(client.trade_cal(exchange='', start_date='19900101', end_date='20181001', fields='exchange,cal_date,is_open,pretrade_date', is_open='0'))

