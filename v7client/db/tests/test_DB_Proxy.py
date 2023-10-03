#!/usr/bin/env python
# -*-coding:utf8-*-
import logging
logging.basicConfig(level=logging.DEBUG)
from unittest import TestCase
from pyodbc import Error

class TestDB_Proxy(TestCase):
    def test_query(self):
        from v7client.db import mssql
        try:
            connection = mssql.MS_Proxy('workdev', '192.168.1.27', 'sa', 'pwd')
            connection.query('select 1')
        except Exception as e:
            print(e.args[0])
            print(e.__class__.__name__)
            print(e.__module__)
            if e.args[0] == '08S01':
                print('woked')
