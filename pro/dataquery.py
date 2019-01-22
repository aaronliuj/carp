#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.sql import table, column, select
import pandas as pd
from . import agent
from .mapper import Calendar, BaseMapper, StockBasic, Stock


class DataQuery:
    def __init__(self):
        self.__agent = agent.Agent()

    def last_calendar(self):
        with self.__agent.session() as s:
            q = s.query(Calendar).order_by(Calendar.cal_date.desc()).limit(1)
            return q.first().cal_date if q.count() > 0 else None


    def get_stock_info(self, code):
        with self.__agent.session() as s:
            return s.query(StockBasic).filter(StockBasic.ts_code == code). first()


    def stock_kline(self, code, freq, start, end):
        t = Stock.create_table(code, freq, self.__agent.meta())
        s = select([t]).where(t.c.trade_date >= start). \
        where(t.c.trade_date <= end). \
        order_by(t.c.trade_date.asc())
        return pd.read_sql(s, self.__agent.db.engine())
        #cls = Stock.create(code, freq, self.__agent.meta())
        #with self.__agent.session() as s:
        #    statment = s.query(cls).filter(cls.trade_date >= start, cls.trade_date <= end).order_by(cls.trade_date.desc())
        #    return pd.read_sql(statment, self.__agent.db.engine())

    def stock_last_date(self, code, freq):
        t = Stock.create_table(code, freq, self.__agent.meta())
        s = select([t]).order_by(t.c.trade_date.desc()).limit(1)

        with self.__agent.db.createConnection() as conn:
            row = conn.execute(s).first()
            if row is None or len(row) == 0:
                return None
            else:
                return row.trade_date

        #cls = Stock.create(code, freq, self.__agent.meta())
        #with self.__agent.session() as s:
        #    q = s.query(cls).order_by(cls.trade_date.desc()).limit(1)
        #    return q.first().trade_date if q.count() > 0 else None

    def empty(self, mapperObject):
        name = mapperObject.tablename()
        return self.__agent.empty(name)


