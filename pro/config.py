#!/usr/bin/env python
# -*- coding: utf-8 -*-


import configparser
import logging

'''


[global]
source=tushare

[tushare]
token=xxxxxx
'''

CONFIG_FILENAME = 'backend.conf'

TUSHARE = 'tushare'

__config = configparser.ConfigParser()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('fb')

try:
    __config.read(CONFIG_FILENAME)
except FileNotFoundError:
    logger.error("File is not found.")



def getSection(name):
    return dict(__config._sections[name]) if __config.has_section(name) else None


def getGlobal():
    return getSection('global')

def getSource():
    name = __config['global'].get('source', TUSHARE)
    src = getSection(name)
    if src is not None:
        return (name, src)
    else:
        raise RuntimeError('not implement')


if __name__ == "__main__":
    logger.info(getSource())
    logger.info(getSection('vvv'))





