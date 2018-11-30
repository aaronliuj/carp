#!/usr/bin/env python
# -*- coding: utf-8 -*-

import config
import pymysql
from abc import ABCMeta, abstractmethod



class MySqlDB(metaclass = ABCMeta):

    __conn = None

    def __init__(self, sectionName):
        self.initialize(sectionName)

    @abstractmethod
    def initialize(self, sectionName):
        conf = config.getOptions(sectionName)
        db = 'default.db'
        if conf is not None:
            db = conf['db']
            if 'db' in conf.keys():
                del conf['db']
        self.__conn = self.__connect(conf)
        self.execute('CREATE DATABASE IF NOT EXISTS %s' % db)
        self.execute('USE %s' % db)


    def query(self, table, columns, **kwargs):
        if self.__conn is None:
            raise IOError('no db  connect')
        sql = 'SELECT' +  self.parse_columns(columns) +  ' FROM ' +  table
        cursor = self.executeWithCursor(sql)
        return cursor.fetchall()

    def insert(self, table, **kwargs):
        pass


    def executeWithCursor(self, sql):
        cursor = self.__conn.cursor()
        cursor.execute(sql)
        return cursor


    def execute(self, sql):
        with self.__conn.cursor() as cursor:
            cursor.execute(sql)
        cursor.close()

    def __connect(self, conf):
        if conf is not None:
            return pymysql.connect(**conf)
        else:
            return pymysql.connect(host = "localhost", user = "root", password = "12345678")


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

class DB(MySqlDB):
    def __init__(self):
        super(DB, self).__init__('mysql')

    def initialize(self, sectionName):
        super().initialize(sectionName)
        self.createTable('table1')

    def createTable(self, name):
        sql = 'CREATE TABLE  IF NOT EXISTS %s (name varchar(8),value varchar(8))' % name
        print(sql)
        self.execute(sql)
       



if __name__ == "__main__":
    db = DB()
    print(db.query("table1", ['name', 'value']))


