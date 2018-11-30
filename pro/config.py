#!/usr/bin/env python
# -*- coding: utf-8 -*-


import configparser

'''


[global]
source=tushare

[tushare]
token=xxxxxx
'''


CONFIG_FILENAME = 'server.conf'


TUSHARE_SECTION_NAME = 'tushare'
MYSQL_SECTION_NAME = 'mysql'

__config = configparser.ConfigParser()

try:
    __config.read(CONFIG_FILENAME)
except FileNotFoundError:
    print("File is not found.")



def getOptions(name):
    return dict(__config._sections[name])

def getSource():
    source = __config['global'].get('source', TUSHARE_SECTION_NAME)
    return getOptions(TUSHARE_SECTION_NAME)


if __name__ == "__main__":
    print(getSource())





