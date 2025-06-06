#!/usr/bin/env python
# -*-coding:utf8-*-
from typing import Iterator, Protocol
from v7client import query_translator, md_reader, dba
from mssql import MsSqlDb
import os
from contextlib import contextmanager
import logging
mylog = logging.getLogger(__name__)

class CursorProtocol(Protocol):
    def fetchall(self) -> Iterator[tuple]:...
    def fetchone(self) -> tuple:...

class Client:
    """
    Хранилище метаданных и методы для работы с 1С базой
    """
    def __init__(self, base_path=None):
        mylog.debug('init Application')
        self.metadata = None
        self.md = None
        self.db = None
        self.load_folder(base_path)

    def load_folder(self, path):
        self.base_path = path
        self.path_md = ''
        self.path_dba = ''
        self.db_server, self.db_name, self.db_user, self.db_password = '', '', '', ''
        if not os.path.isdir(self.base_path):
            raise Exception('Не найден путь к базе')
        mylog.debug(' init from folder: %s' % self.base_path)
        self.path_md  = os.path.join(self.base_path, '1Cv7.MD')
        self.path_dba = os.path.join(self.base_path, '1Cv7.DBA')
        if not os.path.exists(self.path_md):
            raise Exception('Не найден файл 1Cv7.MD')
        self.load_1cv7_md(self.path_md)
        if not os.path.exists(self.path_dba):
            raise Exception('Не найден файл 1Cv7.DBA')
        self.db_server, self.db_name, self.db_user, self.db_password = dba.read_dba(self.path_dba)
        self.db = MsSqlDb(self.db_name, self.db_server, self.db_user, self.db_password)

    def load_1cv7_md(self,filename):
        mylog.debug(' load: %s' % filename)
        self.md = md_reader.parse_md(filename)
        self.metadata = md_reader.extract_metadata(self.md)

    def setDatabase(self, db):
        self.db = db

    @contextmanager
    def query(self, sqltext) -> Iterator[CursorProtocol]:
        with self.db.query(self.parse_query(sqltext)) as cursor:
            yield cursor
                                
    def parse_query(self, sqltext):
        sql = query_translator.prepareSQL(sqltext, self.metadata)
        return sql
