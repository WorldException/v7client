#!/usr/bin/env python
# -*-coding:utf8-*-
import logging
mylog = logging.getLogger(__name__)
import sys
try:
    # pyodbc валится с ошибкой на кодировке, не везде, но на некоторых сборках
    import pypyodbc as pyodbc
    mylog.debug('Use pypyodbc')
except:
    import pyodbc
    mylog.debug('Use pyodbc')

from .db_core import DB_Proxy


class MS_Proxy(DB_Proxy):

    def __init__(self, database, host, user='sa', password='', port=1433):
        DB_Proxy.__init__(self, database, host, user, password, port)

    def connect(self):
        if self.cnx:
            return self.cnx
        if sys.platform == 'win32':
            self.cnx = pyodbc.connect('DRIVER={SQL Server};SERVER=%(host)s;PORT=%(port)s;DATABASE=%(database)s;UID=%(user)s;PWD=%(password)s;CHARSET=UTF8' %
                {
                    'host': self.host,
                    'database': self.database,
                    'user': self.user,
                    'password': self.password,
                    'port': self.port
                }, unicode_results=True)
        else:
            self.cnx = pyodbc.connect('DRIVER={FreeTDS};SERVER=%(host)s;PORT=%(port)s;DATABASE=%(database)s;UID=%(user)s;PWD=%(password)s;TDS version=8.0;ClientCharset=UTF8;' %
                {
                    'host': self.host,
                    'database': self.database,
                    'user': self.user,
                    'password': self.password,
                    'port': self.port,
                }, unicode_results=True)
        return self.cnx
