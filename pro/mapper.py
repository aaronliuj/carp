#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text, Float, Table
#from mysql import BaseMapper
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import mapper


@as_declarative()
class BaseMapper(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    def tablename(self):
        return None

    def dtype(self):
        return None

class Calendar(BaseMapper):
    __tablename__ = 'calendar'
    index = Column(BigInteger, primary_key=True, autoincrement=True)
    exchange = Column(Text)
    cal_date = Column(DateTime)
    is_open = Column(Integer)
    pretrade_date = Column(DateTime)


    def __init__(self):
        pass

    def tablename(self):
        return self.__tablename__

    def dtype(self):
        return {"cal_date": DateTime(),
                "is_open":Integer(),
                "pretrade_date":DateTime()}


    def __repr__(self):
        return "<Calendar(index='%ld', cal_date='%s')>" % (self.index, self.cal_date)


class StockBasic(BaseMapper):
    __tablename__ = 'stock_basic'
    index = Column(BigInteger, primary_key=True)
    ts_code = Column(Text)
    symbol = Column(Text)
    name = Column(Text)
    area = Column(Text)
    industry = Column(Text)
    list_date = Column(DateTime)

    def __init__(self):
        pass

    def tablename(self):
        return self.__tablename__

    def dtype(self):
        return {"list_date": DateTime()}

    def __repr__(self):
        return "<StockBasic (index='%ld', ts_code='%s')>" % (self.index, self.ts_code)




class Stock(object):

    index = Column(BigInteger, primary_key=True)
    ts_code = Column(Text)
    trade_date = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    change = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float)
    amount = Column(Float)


    mapper_table = {}


    #@classmethod
    #def create(cls, code, freq, meta):
    #    t = cls.__create_table(Stock(code, freq).tablename(), meta)
    #    cls = type(code, (Stock,), {})
    #    mapper(cls, t)
    #    return cls

    @classmethod
    def create_table(cls, code, freq, meta):
        return  cls.__create_table(Stock(code, freq).tablename(),
                meta)


    @classmethod
    def __create_table(cls, name, meta):
        if name in Stock.mapper_table:
            return Stock.mapper_table[name]
        else:
            t = Table(name, meta,
                    Column('index', BigInteger, primary_key=True),
                    Column('ts_code', Text),
                    Column('trade_date', DateTime),
                    Column('open', Float),
                    Column('high', Float),
                    Column('close', Float),
                    Column('low', Float),
                    Column('pre_close', Float),
                    Column('change', Float),
                    Column('pct_chg', Float),
                    Column('vol', Float),
                    Column('amount', Float))
            Stock.mapper_table[name] = t
            return t

    def __init__(self, code, freq):
        self.__freq = freq
        self.__code = code


    def tablename(self):
        return "{}_{}".format(self.__code, self.__freq)


    def dtype(self):
        return {
                "trade_date": DateTime(),
                "open": Float(),
                "close": Float(),
                "low": Float(),
                "high": Float(),
                "pre_close": Float()
                }




if __name__ == "__main__":
    s = Stock(1, 'D')










