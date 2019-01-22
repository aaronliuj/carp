#!/usr/bin/env python
# -*- coding: utf-8 -*-
import config
import mysql
import tushare_api
import dateutils as du
import datetime
import mapper
import time
from sqlalchemy.types import Integer, DateTime
from mysql import BaseMapper
from agent import Agent
from dataquery import DataQuery


class DataDownloader:

    def __init__(self):
        self.__agent = Agent()


    def df2table(self, df, mapper, **kwargs):
        table = mapper.tablename()
        dtype = mapper.dtype()
        if table is None:
            raise RuntimeError('error')
        self.__agent.df2table(df, table, dtype = dtype, **kwargs)


    def sync_stock_list(self):
        mod = mapper.StockBasic()
        df = self.__agent.api.get_stock_list()
        self.df2table(df, mod, index = True, if_exists = 'replace')


    def sync_bar(self, code, freq):
        query = DataQuery()
        m = mapper.Stock(code, freq)
        #session = self.__agent.session()
        end = du.today()

        if query.empty(m):
            ## request all bar
            info = query.get_stock_info(code)
            if info is None:
                raise RuntimeError("{} isn't exist".format(code))
            self.__sync_bar_impl(m, code, info.list_date, end, freq)
        else:
            last = query.stock_last_date(code, freq)
            if du.compare(end, last) > 0:
                self.__sync_bar_impl(m, code, du.next(last, freq = freq),
                        end, freq)


    def __sync_bar_impl(self, mapper, code, start ,end, freq):
        ## set chunk_size to make sure get enough data every time
        chunk_size = 3000
        count = du.compare(end, start, freq)
        if count > chunk_size:
            # 分流 (use process or thread?)
            config.logger.info('request with chunk_size = {}'.format(chunk_size))
            _start = start
            _end = du.shift(_start, chunk_size, freq = freq)
            loop_end = False
            while du.compare(_end, _start, freq) > 0 and loop_end == False:
                if du.compare(_end, end, freq) > 0:
                    _end = end
                    loop_end = True
                config.logger.info('request {}->{}'.format(_start, _end))

                df = self.__agent.api.get_bar(code, start_date = _start, end_date = _end, freq = freq)
                self.df2table(df, mapper, index = True, if_exists = 'append')

                # sleep 5 second for next time
                if loop_end == False:
                    _start = du.shift(_start, chunk_size + 1, freq = freq)
                    _end = du.shift(_end, chunk_size + 1, freq = freq)
                    time.sleep(5)

        else:
            df = self.__agent.api.get_bar(code, start_date = start, end_date = end, freq = freq)
            self.df2table(df, mapper, index = True, if_exists = 'append')


    def sync_calendar(self):
        query = DataQuery()
        m = mapper.Calendar()
        sync = True
        freq = 'D'
        if query.empty(m):
            if_exists = 'replace'
            df = self.__agent.api.get_all_calendar()
        else:
            last = query.last_calendar()
            now = du.today()
            if du.compare(now, last, freq = freq) > 0:
                # request new date
                if_exists = 'append'
                df = self.__agent.api.get_all_calendar(start = du.shift(last, 1),
                        end = du.today())
            else:
                sync = False
        if sync and not df.empty:
            self.df2table(df, m, index = False, if_exists = if_exists)

    #def get_calendar(self):
    #    table = 'calendar'
    #    return self.__db.table2df(table)



if __name__ == "__main__":
    da = DataDownloader()
    da.sync_calendar()
    #da.sync_bar('000002.SZ', freq = 'D')








