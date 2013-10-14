#!/usr/bin/env python
# encoding: utf-8

import logging
import dataset
log = logging.getLogger('sw.sql')

DB = dataset.connect('sqlite:///scraperwiki.sqlite')


def save(unique_keys, data, table_name='swdata'):
    global DB
    table = DB[table_name]
    if isinstance(data, dict):
        rows = [data]
    else:
        rows = data

    for row in rows:
        log.debug('upsert({}, {})'.format(row, unique_keys))
        table.upsert(row, unique_keys, types={})
