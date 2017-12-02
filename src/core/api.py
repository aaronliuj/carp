# -*- coding: utf-8 -*-
import os
import datetime
import functools
import pandas as pd
from jaqs.data import DataApi
from urllib.parse import urlencode
from core.util import logger, JsonLoader, LOCAL_CACHE_PATH


def singleton(cls):
    instance = {}

    def geninstance(*args, **kwargs):
        if cls not in instance:
            instance[cls] = cls(*args, **kwargs)
        return instance[cls]
    return geninstance


class BaseDataFrame(object):
    def __init__(self, df):
        self.__df = df

    def set_index(self, key, inplace=True):
        if self.__df is None:
            raise Exception('df is None')
        elif isinstance(self.__df, pd.DataFrame) == False:
            # for debug
            if isinstance(self.__df, int):
                logger.error('df is int %d' % self.__df)
            elif isinstance(self.__df, str):
                logger.error('df is string %s' % self.__df)
            raise Exception('df is not DataFrame ' + type(self.__df))
        elif self.__df.empty is True:
            logger.warning('df is empty')
        elif key in self.__df.keys():
            self.__df.set_index(key, inplace=inplace)

    def raw(self):
        return self.__df

    def query(self, key, value):
        return self.__df[self.__df[key] == value]

    def index(self, i=""):
        if i == "":
            return self.__df.index
        else:
            return self.__df.loc[i]

    def empty(self):
        if self.__df is None:
            return True
        return self.__df.empty

    def __call__(self, key):
        return self.__df[key]

    @staticmethod
    def format_fields(*fields):
        return ','.join(fields)


class InstrumentinfoDataFrame(BaseDataFrame):
    INST_TYPE = 'inst_type'  # 证券类别
    MARKET = 'market'  # 交易所代码
    SYMBOL = 'symbol'  # 证券代码
    NAME = 'name'  # 证券名称
    LIST_DATE = 'list_date'  # 上市日期
    DELIST_DATE = 'delist_date'  # 退市日期
    CNSPELL = 'cnspell'  # 拼音简写
    CURRENCY = 'currency'  # 交易货币
    STATUS = 'status'  # 上市状态,1:上市 3：退市 8：暂停上市
    BUYLOT = 'bylot'  # INT 最小买入单位
    SELLLOT = 'selllot'  # INT 最大买入单位
    PRICETICK = 'pricetick'  # double 最小变动单位
    PRODUCT = 'product'  # 合约品种
    UNDERLYING = 'underlying'  # 对应标的
    MULTIPLIER = 'multiplier'  # int 合约乘数

    def __init__(self, df):
        BaseDataFrame.__init__(self, df)
        self.set_index(InstrumentinfoDataFrame.SYMBOL)

    @classmethod
    def fields(self):
        return BaseDataFrame.format_fields(
            *[self.STATUS,
              self.LIST_DATE,
              self.NAME,
              self.SYMBOL,
              self.MARKET])


class CecsuspDataFrame(BaseDataFrame):
    SYMBOL = 'symbol'  # string 证券代码
    ANN_DATE = 'ann_date'  # string 停牌公告日期
    SUSP_DATE = 'susp_date'  # 停牌开始日期
    SUSP_TIME = 'susp_time'  # string 停牌开始时间
    RESU_DATE = 'resu_date'  # string 复牌日期
    RESU_TIME = 'resu_time'  # string 复牌时间
    SUSP_REASON = 'susp_reason'  # string 停牌原因

    def __init__(self, df):
        BaseDataFrame.__init__(self, df)
        self.set_index(CecsuspDataFrame.SYMBOL)


class SecrestrictedDataFrame(BaseDataFrame):
    SYMBOL = 'symbol'  # string 证券代码
    LIST_DATE = 'list_date'  # string 本期解禁流通日期
    LIFTED_REASON = 'lifted_reason'  # 本期解禁原因（来源）
    LIFTED_SHARES = 'lifted_shares'  # string 本期解禁数量
    LIFTED_RATIO = 'lifted_ratio'  # string 可流通占A股总数比例

    def __init__(self, df):
        BaseDataFrame.__init__(self, df)
        self.set_index(SecrestrictedDataFrame.SYMBOL)


class TradecalDataFrame(BaseDataFrame):
    TRADE_DATE = 'trade_date'  # int YYYYMMDD格式，如20170823
    ISTRADEDAY = 'istradeday'  # string 是否交易日
    ISWEEKDAY = 'isweekday'  # string 是否工作日
    ISWEEKDAY = 'isweekend'  # string 是否周末
    ISHOLIDAY = 'isholiday'  # string string 否节假日

    def __init__(self, df):
        BaseDataFrame.__init__(self, df)
        self.set_index(TradecalDataFrame.TRADE_DATE)


class DailyDataFrame(BaseDataFrame):
    SYMBOL = 'symbol'
    CODE = 'code'  # string 交易所原始代码
    TRADE_DATE = 'trade_date'  # int YYYYMMDD格式，如20170823
    OPEN = 'open'  # double 开盘价
    HIGH = 'high'  # double 最高价
    LOW = 'low'  # double 最低价
    CLOSE = 'close'  # double 收盘价
    VOLUME = 'volume'  # volume double 成交量
    TURNOVER = 'turnover'  # turnover double 成交金额
    VWAP = 'vwap'  # double 成交均价
    SETTLE = 'settle'  # double 结算价
    OI = 'oi'  # double 持仓量
    TRADE_STATUS = 'trade_status'  # string 交易状态（”停牌”或者”交易”）
    TRADE_SUSPENSION = '停牌'
    TRADE_TRANSACTION = '交易'

    def __init__(self, df):
        BaseDataFrame.__init__(self, df)
        self.set_index(DailyDataFrame.TRADE_DATE)


class BarDataFrame(BaseDataFrame):
    SYMBOL = 'symbol'
    CODE = 'code'  # string 交易所原始代码
    DATE = 'date'  # int
    TIME = 'time'  # int
    TRADE_DATE = 'trade_date'  # int YYYYMMDD格式，如20170823
    FREQ = 'freq'  # bar 类型
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'
    TURNOVER = 'turnover'
    VWAP = 'vwap'
    OI = 'oi'
    SETTLE = 'settle'

    def __init__(self, df):
        BaseDataFrame.__init__(self, df)
        self.set_index(BarDataFrame.TRADE_DATE)


@singleton
class Api(object):
    __api = DataApi(addr='tcp://data.tushare.org:8910')

    def __lazy_login(self):
        if self.__login_flag is False:
            ret = self.__api.login(self.id, self.token)
            if ret[0] is None:
                raise Exception('login failed')
            else:
                logger.info('%s login success' % self.id)
            self.__login_flag = ret

    def __init__(self):
        self.__login_flag = False
        l = JsonLoader.create('.config.json')
        self.id = l.get('id')
        self.token = l.get('token')

    # 获取市场股市列表
    def instrumentinfo(self, _fields=InstrumentinfoDataFrame.fields(), _filter="inst_type=1&status=1&market=SH,SZ"):
        self.__lazy_login()
        df, msg = self.__api.query(view="jz.instrumentInfo", fields=_fields,
                                   filter=_filter,
                                   data_format='pandas')
        logger.debug('request jz.instrumentInfo')
        return InstrumentinfoDataFrame(df)

    # 停复牌resu_date 未设置
    # def secsusp(self, _filter="", _fields=""):
    #    self.__lazy_login()
    #    df, msg = self.__api.query(
    #        view="lb.secSusp",
    #        fields=_fields,
    #        filter=_filter,
    #        data_format='pandas')
    #    logger.debug('request lb.secSusp')
    #    return CecsuspDataFrame(df)

    # 获取限售股解禁列表
    def secrestricted(self, _start_date, _end_date, _fields=""):
        self.__lazy_login()
        filters = urlencode({'start_date': TradeCalendar.date2int(_start_date),
                             'end_date': TradeCalendar.date2int(_end_date)})
        df, msg = self.__api.query(
            view="lb.secRestricted",
            fields=_fields,
            filter=filters,
            data_format='pandas')
        logger.debug('request lb.secRestricted')
        return SecrestrictedDataFrame(df)

    # 日交易行情
    def daily(self, _symbol, _start_date, _end_date, _freq='1d', _fields=BaseDataFrame.format_fields(DailyDataFrame.CLOSE,
                                                                                                     DailyDataFrame.TRADE_DATE,
                                                                                                     DailyDataFrame.OPEN,
                                                                                                     DailyDataFrame.HIGH,
                                                                                                     DailyDataFrame.LOW,
                                                                                                     DailyDataFrame.VOLUME,
                                                                                                     DailyDataFrame.TRADE_STATUS), _adjust_mode='post'):
        self.__lazy_login()
        df, msg = self.__api.daily(
            symbol=_symbol,
            freq=_freq,
            start_date=TradeCalendar.date2string(_start_date),
            end_date=TradeCalendar.date2string(_end_date),
            fields=_fields,
            adjust_mode=_adjust_mode)
        logger.debug('request daily')
        return DailyDataFrame(df)

    # 交易日历
    def tradecal(self, _fields="trade_date, istradeday"):
        self.__lazy_login()
        df, msg = self.__api.query(
            view="jz.secTradeCal",
            fields=_fields)
        logger.debug('request jz.secTradeCal')
        return TradecalDataFrame(df)

    # 订阅
    def subscribe(self, _symbol, _func, _fields):
        self.__lazy_login()
        sublist, msg = self.__api.subscribe(_symbol, func=_func,
                                            fields=_fields)
        logger.debug('request subscribe')
        return sublist

    # bar
    def bar(self, _symbol, _trade_date, _freq="5M", _fields=""):
        self.__lazy_login()
        df, msg = self.__api.bar(symbol=_symbol,
                                 trade_date=_trade_date,
                                 freq=_freq,
                                 fields=_fields)
        logger.debug('request bar')
        return BarDataFrame(df)

    # industry failed
    # def industry(self, **industrys):
    #    df, msg = self.__api.query(
    #            view="lb.secIndustry",
    #            fields="",
    #            #filter="industry1_name=金融&industry2_name=金融&industry_src=中证",
    #            #filter="symbol=000001.SH&industry_src=中证",
    #            data_format='pandas')
    #    return df

    # indicator failed
    # def indicator(self):
    #    df, msg = self.__api.query(view="wd.secDailyIndicator",
    #            filter='symbol=000001.SZ&start_date=20170605&end_date=20170701',
    #            fields="",
    #            data_format='pandas')
    #    return df

    # indexinfo failed
    # def indexInfo(self, _symbol, _fields = ""):
    #    df, msg = self.__api.query(
    #            view="lb.indexInfo",
    #            fields=_fields,
    #            filter="",
    #            #filter="index_code=399001",
    #            #filter="symbol=000001.SH",
    #            data_format='pandas')
    #    return df
    #    #return BarDataFrame(df)

    # @staticmethod
    # def format_filter(**filters):
    #     return urlencode(filters)


class TradeCalendar(object):
    __api = Api()
    __filename = 'celendar.json'
    __calendar = None

    @classmethod
    def __lazy_load(cls):
        if TradeCalendar.__calendar is None:
            filename = os.path.join(LOCAL_CACHE_PATH, cls.__filename)
            loader = JsonLoader.create(filename)

            def load_func(key, data):
                if data is None:
                    trade = cls.__api.tradecal()
                    loader.put(infos=trade.raw().to_json(orient='index'))
                    loader.sync()
                    return trade
                else:
                    return TradecalDataFrame(pd.read_json(data, orient='index'))

            TradeCalendar.__calendar = loader.get('infos', load_callback=load_func)

    @classmethod
    def duration(cls, _date="", _days=0, _trade=True):
        now = datetime.datetime.now()
        start = cls.day(_date=_date, _trade=_trade)
        end = cls.day(_date=_date, _days=_days, _trade=_trade)
        return (start, end) if start < end else (end, start)

    @classmethod
    def now(cls, _trade=True):
        return cls.day(_trade=_trade)

    @classmethod
    def day(cls, _date="",  _days=0, _trade=True):
        cls.__lazy_load()
        now = datetime.datetime.now()
        if isinstance(_date, int):
            now = now if _date == 0 else cls.int2date(_date)
        elif isinstance(_date, str):
            now = now if _date == "" else cls.string2date(_date)

        if _trade:
            df = cls.__calendar.raw()
            now2int = cls.date2int(now)
            if _days <= 0:
                now2int = df[df.index.astype('int64') <= now2int].iloc[-1 + _days].name
            else:
                now2int = df[df.index.astype('int64') >= now2int].iloc[_days].name
            return cls.int2date(int(now2int))
        else:
            return now + datetime.timedelta(days=_days)

    @staticmethod
    def date2int(date):
        return date.year * 10000 + date.month * 100 + date.day

    @staticmethod
    def int2date(num):
        year = num // 10000
        month = (num - (num - num % 10000)) // 100
        day = num % 100
        return datetime.datetime(year, month, day)

    @staticmethod
    def date2string(date):
        FORMAT = '%Y-%m-%d'
        return date.strftime(FORMAT)

    @staticmethod
    def datestr2int(s):
        date = TradeCalendar.string2date(s)
        return TradeCalendar.date2int(date)

    @staticmethod
    def string2date(s):
        FORMAT = '%Y-%m-%d'
        return datetime.datetime.strptime(s, FORMAT)


if __name__ == "__main__":
    #api = Api()
    #print(api.daily('000006.SZ', TradeCalendar.day(_days=-1440), TradeCalendar.day(), _freq='1d').raw())

    filters = urlencode({'start_date': '3',
                         'end_date': '4'})
    # print(api.secsusp(_filter="end_date=20180125"))
    #print(api.bar('000001.SZ', '2018-01-17', _freq = '5M').raw())
    # print(TradeCalendar.day(_days=-3))
