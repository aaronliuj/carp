#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import time
from sqlalchemy.types import Integer, DateTime
import pandas as pd
from contextlib import contextmanager
from . import config
from . import mysql
from . import tushare_api
from . import dateutils as du
from . import mapper
from .mysql import BaseMapper

class Agent:
    db = None
    api = None
    def __init__(self):
        ## TODO:
        self.init_db()
        self.init_api()

    def init_db(self):
        db = config.getGlobal()['db']
        if db is None:
            raise RuntimeError("db isn't set")
        if db == 'mysql':
            self.__conf = config.getSection(db)
            self.db = mysql.MySqlDB.get(self.create_url(self.__conf))
        else:
            raise NotImplementedError()

    def init_api(self):
        name, src = config.getSource()
        if name == 'tushare':
            self.api = tushare_api.TuShareApi.get(src['token'])
        else:
            raise NotImplementedError(name)


    @contextmanager
    def session(self):
        s = self.db.createSession()
        try:
            yield s
            s.commit()
        except:
            s.rollback()
            raise
        finally:
            s.close()

    def meta(self):
        return self.db.meta()


    def df2table(self, df, tablename, **kwargs):
        pd.io.sql.to_sql(df, tablename,
                self.db.engine(),
                schema= self.get_db_name(), **kwargs)


    def table2df(self, tablename):
        with self.db.createConnection() as conn:
            data = pd.read_sql_table(tablename, conn)
        return data


    def get_db_name(self):
        return self.__conf['db']



    def dfquery(self, tablename, col = None, where = None, order = None, desc = False, limit = None):
        sql = 'select {} from {}'.format('*' if col is None else col, tablename)
        if where is not None:
            sql += " where {} ".format(where)

        if order is not None:
            sql += " order by {} ".format(order)
            if desc == False:
                sql += " asc "
            else:
                sql += " desc "

        if limit is not None:
            sql += " limit {} ".format(1)

        with self.db.createConnection() as conn:
            return pd.read_sql_query(sql, conn, index_col = None)


    def create_url(self, conf):
        if conf:
            return "mysql+pymysql://{}:{}@{}:{}/{}".format(conf['user'], conf['password'], conf['host'], conf['port'], conf['db'])
        else:
            raise RuntimeError("no db")


    def empty(self, name):
        ## FIXME: need refine
        if not self.db.engine().has_table(name):
            return True
        else:
            #print(self.db.engine().dialect.table)
            with self.db.createConnection() as con:
                rs = con.execute('SELECT * FROM `{}` LIMIT 1'.format(name))
                return rs.first() is None



