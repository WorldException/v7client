#!/usr/bin/env python
#-*-coding:utf8-*-
from __future__ import unicode_literals
from typing import Iterable, Dict, List, Iterator, Union, Any, Tuple
import logging
from .query_translator import prepareSQL
from . import mssql
from . import md_reader2
from . import dba
import os
from datetime import datetime
import pickle
import six
import re
from typing import Type, NamedTuple, TypeVar, Generic, TypedDict
from .config import Config, MsSqlConfig
from collections import OrderedDict, namedtuple
import re
from .smb import PatchedSmbClient

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

    def __init__(self, config: Config, caching=True, use_dba=True):
        """
        :param config: конфигурация
        :param caching: кешировать метаданные для повторного использования
        """
        self.caching = caching
        self.__last_md_ctime = None

        self.config = config
        self.connection = None
        self.reader = None
        self.metadata = None
        self.use_dba = use_dba
        self._dba: MsSqlConfig | None = None

        # путь к кешу структуры конфигурации
        self.metadata_pickled_file = self.config.get_full_store_path('metadata.pkl')

        # Если нет конфига то скачиваю
        if not os.path.exists(self.config.PATH_1Cv7_MD):
            mylog.info('config not found %s, start downloading' % self.config.PATH_1Cv7_MD)
            self.download()

    @property
    def name(self):
        return self.config.NAME

    def download(self):
        # обновленияе 1с-ых файлов
        self.update_meta_files()
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

    def load_metadata(self) -> bool:
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

    def load_dba(self) -> MsSqlConfig:
        # считывает параметры подключения к базе из 1cv7.dba
        db = dba.read_dba(self.config.PATH_1Cv7_DBA, True)
        return MsSqlConfig(db['UID'], db['PWD'], db['Server'], db['DB'])
    
    def get_mssql_config(self)-> MsSqlConfig:
        if self.use_dba:
            if not self._dba:
            # считывание параметров подключения
            # из файла
                self._dba = self.load_dba()
            return self._dba
        else:
            return self.config.MSSQL_CONFIG

    def connect(self):
        db = self.get_mssql_config()
        return mssql.MsSqlDb(db.SQL_DB, db.SQL_HOST, db.SQL_USER, db.SQL_PWD)

    def query(self, sql) -> 'Query':
        """
        :param sql:
        :return: Query
        """
        self.lazy_read_config()
        return Query(sql, self)

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
    
    def update_meta_files(self):
        #ToDo добавить лок для скачивание только одним потоком
        mylog.info(f'Start update meta files: {self}')
        if self.config.PATH_TYPE == 'smb':
            smb = self.smbclient
            workdir = smb.listdir(self.config.PATH_TO_BASE, '1*7.*')
            mylog.info(u'Files on smb: %s' % repr(workdir))
            found_md = False
            for filename in self.config.FILES:
                target_filename = filename.lower()
                
                if filename in workdir:
                    fullname = os.path.join(self.config.PATH_TO_BASE, filename)
                    target_name = self.config.get_full_store_path(target_filename)
                    mylog.info(u'downloading: %s -> %s' % (fullname, target_name))
                    smb.download(fullname, target_name)
                    if target_filename == '1cv7.md':
                        found_md = True

            if not found_md:
                mylog.warning('1Cv7.md not found')
                for fn in workdir:
                    mylog.debug(fn)
            return True

        if self.config.PATH_TYPE == 'dir':
            workdir = os.listdir(self.config.PATH_TO_BASE)
            mylog.debug(u'Files on smb: %s' % repr(workdir))
            for filename in self.config.FILES:
                target_filename = filename.lower()
                if filename in workdir:
                    fullname = os.path.join(self.config.PATH_TO_BASE, filename)
                    target_name = self.config.get_full_store_path(target_filename)
                    mylog.info(u'copying: %s -> %s' % (fullname, target_name))
                    with open(fullname, 'rb', 1024) as fsrc:
                        with open(target_name, 'wb', 1024*1024) as fout:
                            fout.write(fsrc.read())
            return True

    @property
    def smbclient(self):
        return PatchedSmbClient(server=self.config.SMB_SERVER, share=self.config.SMB_SHARE, username=self.config.SMB_USER, password=self.config.SMB_PWD)

    def download_smb(self, remote, target, force=False) -> True:
        """
        Скачать новый файл. Проверяет файл на изменения времени создания и размера.

        :param remote: путь относительно базы на удаленном сервере
        :param target: куда сохранить
        :param force:
        :return: True если скачан более новый файл
        """
        remote_full_path = os.path.join(self.config.PATH_TO_BASE, remote)
        mylog.debug(f"remote file:{remote_full_path}")
        smb = self.smbclient
        info = smb.info(remote_full_path)
        #{'altname': 'IMPORT~1.CSV', 'create_time': 'Wed Jul 10 12:51:33 2019 MSK', 'access_time': 'Thu May  7 12:16:05 2020 MSK', 'write_time': 'Fri Nov 26 21:54:06 2021 MSK', 'change_time': 'Fri Nov 26 21:54:06 2021 MSK', 'attributes': 'NA (2020)', 'stream': '[::$DATA], 9133499 bytes'}
        try:
            remote_time = datetime.strptime(info['change_time'], '%a %b %d %H:%M:%S %p %Y %Z')
        except:
            remote_time = datetime.strptime(info['change_time'], '%a %b %d %H:%M:%S %Y %Z')
        remote_size = int(re.findall(r"(\d+)\sbytes", info['stream'], re.IGNORECASE)[0])
        mylog.debug(info)
        mylog.debug(f"change time:{remote_time}; size:{remote_size}")
        target_mtime = datetime(1,1,1)
        target_size = 0
        if os.path.exists(target):
            target_mtime = datetime.fromtimestamp(os.path.getmtime(target))
            target_size = int(os.path.getsize(target))
            mylog.debug(f"target time:{target_mtime}; size:{target_size}")
        if target_mtime < remote_time or target_size != remote_size or force:
            smb.download(remote_full_path, target)
            target_size = os.path.getsize(target)
            mylog.debug(f"target time:{target_mtime}; size:{target_size}")
            return remote_size == target_size

        return False

T = TypeVar('T')

class Query:
    """
    объект запроса, объединяет транслятор и выполение запроса
    """
    def __init__(self, sql:str, base:Base):
        self.sql = sql  # свойство или переменная?
        self._sql_v7_ = ''
        self.params = {}
        self.base = base

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
            self._sql_v7_ = prepareSQL(self.sql, self.base.metadata)
        return self._sql_v7_ % self.params

    @v7.setter
    def v7(self, value):
        self._sql_v7_ = value

    def __str__(self):
        return f"SQL:\n{self.sql}\nV7\n{self.v7}"

    def __repr__(self):
        return self.__str__()

    def __call__(self, **params) -> Iterator[tuple]:
        """
        выполнение запроса
        :param args:
        :param kwargs: параметры для запроса
        :return:
        """
        # можно использовать одно и то же подключение
        if params:
            self.set_params(**params)
        sql_text = self.v7
        sqllog.debug(sql_text)
        cnx = self.base.connect()
        with cnx.query(sql_text) as cursor:
            for row in cursor:
                yield row
    
    def as_dict_list(self, **params):
        return self.as_list(dict, **params)

    def as_list(self, result_type:Type[T], **params) -> Iterator[T]:
        
        if params:
            self.set_params(**params)
        cnx = self.base.connect()
        with cnx.query(self.v7) as cursor:
            cols = [col[0] for col in cursor.description]
            for row in cursor:
                yield result_type(zip(cols, row))

    def as_object_list(self, **params) -> Iterator[NamedTuple]:
        cursor = self.__call__(**params)
        try:
            cols = [str(col[0]) for col in cursor.description]
            row_type = namedtuple('rowtype', ' '.join(cols), rename=True)
            for item in cursor:
                yield row_type(*item)
        finally:
            cursor.close()

    @staticmethod
    def as_datetime(value) -> datetime | str:
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
