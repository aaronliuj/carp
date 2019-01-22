#!/usr/bin/env python
# -*- coding: utf-8 -*-

from . import config
#import pymysql
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from .mapper import BaseMapper


class MySqlDB:
    dbs = {}

    @classmethod
    def get(cls, url):
        if url in cls.dbs:
            return cls.dbs[url]
        else:
            cls.dbs[url] = MySqlDB(url)
            return cls.dbs[url]


    def __init__(self, url):
        self.__engine = sqlalchemy.create_engine(url, encoding="utf8", echo = False)
        self.__session = sessionmaker(bind = self.__engine)
        self.__metadata = sqlalchemy.MetaData()
        self.__metadata.create_all(bind = self.__engine)
        BaseMapper.metadata.create_all(self.__engine)


    def createSession(self):
        return self.__session()

    def createConnection(self):
        return self.__engine.connect()

    def engine(self):
        return self.__engine


    def meta(self):
        return self.__metadata


    def tableExist(self, name):
        return self.__engine.dialect.has_table(self.__engine, name)


    '''
    def parse_columns(self, columns):
        str = ' '
        if columns is not None:
            i = 0
            for c in columns:
                if i > 0:
                    str += " , "
                str += ' %s ' % c
                i += 1
        else:
            str = ' * '
        return str
    '''

if __name__ == "__main__":
    pass
