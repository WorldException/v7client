#!/usr/bin/env python
#-*-coding:utf8-*-
from __future__ import unicode_literals
from typing import Iterable, Dict, List, Iterator, Union, Any, Tuple
import logging
from .query_translator import prepareSQL
from .db import mssql
from . import md_reader2
from . import dba
import os
from datetime import datetime
import pickle
import six
import re
from typing import Type, NamedTuple, TypeVar, Generic, TypedDict
from .config import BaseConfig
from collections import OrderedDict, namedtuple

mylog = logging.getLogger(__name__)
sqllog = logging.getLogger('v7.sql')


def date_param(datetimeparam, end_modificator=False):
    s = datetimeparam.strftime('%Y%m%d')
    if end_modificator:
        s = s + 'Z'
    return "'%s'" % s


class Base(object):
    """
    это главный класс для работы с базой данных, передаем конфиг и можем вызвать запросы
    """

    def __init__(self, config: Type[BaseConfig], caching=True):
        """
        :param config: конфигурация
        :param caching: кешировать метаданные для повторного использования
        """
        self.caching = caching
        self.__last_md_ctime = None

        self.config = config()
        self.connection = None
        self.reader = None
        self.metadata = None
        self.dba_info = None

        # путь к кешу структуры конфигурации
        self.metadata_pickled_file = self.config.METAFILE_FULL_PATH('metadata.pkl')

        # Если нет конфига то скачиваю
        if not os.path.exists(self.config.PATH_1Cv7_MD):
            mylog.info('config not found %s, start downloading' % self.config.PATH_1Cv7_MD)
            self.download()

    @property
    def name(self):
        return self.config.NAME

    def download(self):
        # обновленияе 1с-ых файлов
        self.config.update_meta_files()
        # удаление предыдущей инфы о метаданных конфига
        if self.caching and os.path.exists(self.metadata_pickled_file):
            os.remove(self.metadata_pickled_file)

    def lazy_read_config(self):
        """
        ленивая загрузка из метаданных или конфигурации
        :return:
        """
        # если данные устарели то скачать файл и обновить метаданные
        if self.metadata_expired():
            self.download()
            self.metadata = None

        if self.metadata is None:
            if not self.load_metadata():
                self.read_config()
                self.save_metadata()
        return self.metadata

    @property
    def md(self):
        """
        доступ к метаданным с подгрузкой в случае необходимости
        :return:
        """
        return self.lazy_read_config()

    def metadata_expired(self):
        """ проверить что файл конфигурации требует обновления
        :return True - данные устарели
        """
        if self.config.UPDATE_INTERVAL:
            if self.__last_md_ctime is None:
                if os.path.exists(self.config.PATH_1Cv7_MD):
                    stat = os.stat(self.config.PATH_1Cv7_MD)
                    dt = datetime.fromtimestamp(stat.st_ctime)
                    self.__last_md_ctime = dt
            if self.__last_md_ctime is not None:
                delta = datetime.now() - self.__last_md_ctime
                if delta > self.config.UPDATE_INTERVAL:
                    mylog.info('metadata expired')
                    return True
        return False

    def save_metadata(self):
        if self.caching and not self.metadata is None:
            if os.path.exists(self.metadata_pickled_file):
                mylog.info('file exist, rewrite: %s' % self.metadata_pickled_file)
            with open(self.metadata_pickled_file, 'wb') as f:
                pickle.dump(self.metadata, f)

    def load_metadata(self):
        if self.caching and os.path.exists(self.metadata_pickled_file):
            try:
                with open(self.metadata_pickled_file, 'rb') as f:
                    self.metadata = pickle.load(f)
                return True
            except Exception as e:
                mylog.error('cant load pickled metadata %s' % repr(e))
                self.metadata = None
        return False

    def read_config(self):
        # загрузка конфигурации
        self.reader = md_reader2.MdReader(self.config.PATH_1Cv7_MD).read()
        self.metadata = self.reader.MdObject

    def connect(self):
        # подготовка подключения
        if not self.dba_info:
            # считывание параметров подключения
            if self.config.SQL_HOST and self.config.SQL_PWD and self.config.SQL_USER and self.config.SQL_DB:
                # из конфига
                self.dba_info = dict(DB=self.config.SQL_DB,
                                     Server=self.config.SQL_HOST,
                                     UID=self.config.SQL_USER,
                                     PWD=self.config.SQL_PWD)
            else:
                # из файла
                self.dba_info = dba.read_dba(self.config.PATH_1Cv7_DBA, True)
                mylog.info('read DBA: %s' % repr(self.dba_info))
        return mssql.MS_Proxy(self.dba_info['DB'], self.dba_info['Server'], self.dba_info['UID'], self.dba_info['PWD'])

    def prepare_connection(self):
        # подготовка подключения
        self.connection = self.connect()
        return self.connection

    def query(self, sql) -> 'Query':
        """
        :param sql:
        :return: Query
        """
        self.lazy_read_config()
        return Query(sql, self)

    def add_query(self, **kwargs):
        """
        добавить метод запрос
        """
        pass

    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                mylog.error(e)
            self.connection = None

    def x(self, alias):
        return self.md.x(alias)

    def get_object(self, obj_type, obj_name):
        """
        получить объект
        :param obj_type: справочник
        :param obj_name: контрагенты
        :return:
        """
        objs = self.md._get_by_alias(obj_type)
        for obj in objs:
            if obj._name.lower() == obj_name.lower():
                return obj
        return None

ResultTypedDict = TypeVar('ResultTypedDict', bound=Type[Union[dict, TypedDict]])
T = TypeVar('T')

class Query:
    """
    объект запроса, объединяет транслятор и выполение запроса
    """
    def __init__(self, sql:str, parent:Base):
        self.sql = sql  # свойство или переменная?
        self._sql_v7_ = ''
        self.params = {}
        self.parent = parent

    def set_param(self, name: str, value:Union[str, int, float, List, Tuple, datetime]):
        """
        :param name: name_ означает что используется модификатор как в 1С++ ~ для обозначения даты конец дня
        :param value:
        :return:
        """
        modificator = name.endswith('_')
        if modificator:
            name = name[:-1]
        if type(value) is datetime:
            self.params[name] = date_param(value, modificator)
        elif type(value) in (list, tuple):
            """ скорее всего список для in (%(value)s) """
            self.params[name] = ','.join([f"'{x}'" for x in value])
        else:
            self.params[name] = value

    def set_params(self, **kwargs):
        for key, value in six.iteritems(kwargs):
            self.set_param(key, value)

    @property
    def sql(self):
        return self._sql_

    @sql.setter
    def sql(self, value):
        self._sql_v7_ = ''
        self._sql_ = value

    @property
    def v7(self) -> str:
        if not self._sql_v7_:
            self._sql_v7_ = prepareSQL(self.sql, self.parent.metadata)
        return self._sql_v7_ % self.params

    @v7.setter
    def v7(self, value):
        self._sql_v7_ = value

    def __unicode__(self):
        return "SQL:\n%s\nV7\n%s" % (self.sql, self.v7)

    def __str__(self):
        return "SQL:\n%s\nV7\n%s" % (self.sql, self.v7)

    def __repr__(self):
        return self.__unicode__()

    def __call__(self, **kwargs):
        """
        выполнение запроса
        :param args:
        :param kwargs: параметры для запроса
        :return:
        """
        # можно использовать одно и то же подключение
        connection = kwargs.pop('connection', self.parent.connect())
        if kwargs:
            self.set_params(**kwargs)
        sqllog.debug(self.v7)
        if six.PY2:
            # база в cp1251
            sql_cp1251 = self.v7.encode('cp1251')
        else:
            sql_cp1251 = self.v7
        return connection.query(sql_cp1251)
    
    def as_dict_list(self, **kwargs):
        return self.as_list(dict, **kwargs)

    def as_list(self, result_type:Type[T]=Tuple[Any], **kwargs) -> Iterable[T]:
        cnx = self.parent.connect()
        cursor = self.__call__(connection=cnx, **kwargs)
        try:
            cols = [col[0] for col in cursor.description]
            for item in cursor:
                yield result_type(zip(cols, item))
        finally:
            cursor.close()
            cnx.close()

    def as_object_list(self, **kwargs) -> Iterator[NamedTuple]:
        cursor = self.__call__(**kwargs)
        try:
            cols = [str(col[0]) for col in cursor.description]
            row_type = namedtuple('rowtype', ' '.join(cols), rename=True)
            for item in cursor:
                yield row_type(*item)
        finally:
            cursor.close()

    def as_DataFrame(self, **kwargs):
        import pandas
        return pandas.DataFrame(self.as_dict_list(**kwargs))

    @staticmethod
    def as_datetime(value):
        """
        преобразовать данные даты 1с в datetime
        гггг-мм-дд
        :param value: 1961-07-09 00:00:00.000
        :return: datetime
        """
        g = re.findall(r'(\d{4})-(\d{2})-(\d{2})', value)
        if g:
            return datetime(year=int(g[0][0]), month=int(g[0][1]), day=int(g[0][2]))
        return ''
