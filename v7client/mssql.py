#!/usr/bin/env python
# -*-coding:utf8-*-
import logging
from typing import Generator
import pytds
from contextlib import contextmanager

mylog = logging.getLogger(__name__)
logging.getLogger("pytds").setLevel(logging.WARNING)


class MsSqlDb:

    def __init__(self, database, host, user='sa', password='', port=1433, **connect_args):
        self.cnx = None
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database
        self.connect_args = connect_args

    def connect(self) -> pytds.Connection:
        return pytds.connect(self.host, self.database, self.user, self.password, port=self.port, bytes_to_unicode=True, **self.connect_args)

    @contextmanager
    def query(self, sql) -> Generator[pytds.Cursor, None, None]:
        with self.connect() as cnx:
            with cnx.cursor() as cursor:
                cursor.execute(sql)
                yield cursor

    @contextmanager
    def cursor(self) -> Generator[pytds.Cursor, None, None]:
        with self.connect() as cnx:
            with cnx.cursor() as cursor:
                yield cursor
