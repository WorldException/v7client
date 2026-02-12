#!/usr/bin/env python
#-*-coding:utf8-*-
import logging
import os
from datetime import timedelta, datetime
import tempfile
from dataclasses import dataclass
from typing import Literal, LiteralString

log = logging.getLogger(__name__)


def getpath(filename=''):
    return os.path.join(os.path.dirname(__file__), filename)

@dataclass(init=True, repr=True)
class MsSqlConfig:
    # подключение к скл
    SQL_USER: str
    SQL_PWD: str
    SQL_HOST: str
    SQL_DB: str
    SQL_DB_PORT: int = 1433


class Config:
    NAME = 'default'
    # доступ к папке с конфигурацией
    PATH_TYPE = 'dir' # smb|dir|ftp

    def __init__(self, name:str, 
                path_to_base:str='/',
                sql_user:str='', sql_pwd:str='', sql_host:str='', sql_db:str='',
                path_type: Literal['smb', 'dir', 'ftp']='dir', 
                smb_server=None, smb_share=None, smb_user=None, smb_pwd=None, 
                update_interval=timedelta(0),
                file_1cv7_md:str='1Cv7.MD', file_1cv7_dds:str='1Cv7.DDS', file_1cv7_dba:str='1Cv7.DBA', sql_db_port=1433):
        
        self.NAME = name

        self.SMB_SERVER = smb_server
        self.SMB_PWD = smb_pwd
        self.SMB_SHARE = smb_share
        self.SMB_USER = smb_user

        if path_type is None and self.SMB_SERVER:
            self.PATH_TYPE = 'smb'
        else:
            self.PATH_TYPE = path_type

        self.MSSQL_CONFIG = MsSqlConfig(sql_user, sql_pwd, sql_host, sql_db, int(sql_db_port))
        # self.SQL_DB = sql_db or self.SQL_DB
        # self.SQL_HOST = sql_host or self.SQL_HOST
        # self.SQL_USER = sql_user or self.SQL_USER
        # self.SQL_PWD = sql_pwd or self.SQL_PWD

        self.PATH_TO_BASE = path_to_base
        self.FILE_1Cv7_MD = file_1cv7_md
        self.FILE_1Cv7_DDS = file_1cv7_dds
        self.FILE_1Cv7_DBA = file_1cv7_dba
        
        self.UPDATE_INTERVAL: timedelta = update_interval

        self.prepare_paths()

    @classmethod
    def build_from_env(cls) -> 'Config':
        ptype = 'dir'
        if os.environ.get('V7_PATH_TYPE', '') == 'smb':
            ptype = 'smb'
        return cls(
            name=os.environ.get('V7_NAME', 'v7base'),
            path_type=ptype,
            
            smb_server=os.environ.get('V7_SMB_SERVER', ''),
            smb_share=os.environ.get('V7_SMB_SHARE', ''),
            smb_user=os.environ.get('V7_SMB_USER', ''),
            smb_pwd=os.environ.get('V7_SMB_PWD', ''),
            
            sql_user=os.environ['V7_SQL_USER'],
            sql_pwd=os.environ['V7_SQL_PWD'],
            sql_host=os.environ['V7_SQL_HOST'],
            sql_db=os.environ['V7_SQL_DB'],
            sql_db_port=int(os.environ.get('V7_SQL_DB_PORT', 1433)),

            path_to_base=os.environ.get('V7_PATH_TO_BASE', '/'),
            file_1cv7_md=os.environ.get('V7_FILE_1Cv7_MD', '1Cv7.MD'),
            file_1cv7_dds=os.environ.get('V7_FILE_1Cv7_DDS', '1Cv7.DDS'),
            file_1cv7_dba=os.environ.get('V7_FILE_1Cv7_DBA', '1Cv7.DBA'),
        )

    @property
    def FILES(self):
        return (self.FILE_1Cv7_MD, self.FILE_1Cv7_DDS, self.FILE_1Cv7_DBA, '1cv7.md')

    @property
    def PATH_META(self):
        # общий путь к папке с метаданными
        return os.getenv('V7_PATH_META', os.path.join(tempfile.gettempdir(), 'v7_metadata'))

    @property
    def PATH_META_DIR(self):
        # полный путь к папке метаданных 1С с учетом имени (для этой базы)
        return os.path.join(self.PATH_META, self.NAME)
    
    @property
    def PATH_TO_MODEL(self):
        # полный путь к моделям на py
        return os.path.join(self.PATH_META_DIR, 'model')
    
    # абсолютные пути к файлам после инициализации
    @property
    def PATH_1Cv7_DBA(self):
        return self.get_full_store_path(self.FILE_1Cv7_DBA.lower())
    
    @property
    def PATH_1Cv7_DDS(self):
        return self.get_full_store_path(self.FILE_1Cv7_DDS.lower())
    
    @property
    def PATH_1Cv7_MD(self):
        return self.get_full_store_path(self.FILE_1Cv7_MD.lower())

    
    def get_full_store_path(self, filename):
        return os.path.join(self.PATH_META_DIR, filename)

    def prepare_paths(self):
        for dirpath in (self.PATH_META_DIR, self.PATH_TO_MODEL):
            if not os.path.exists(dirpath):
                log.info(u'Create dir for %s=%s' % (self.__class__.__name__, dirpath))
                os.makedirs(dirpath)

    def __str__(self):
        return u"1Cv7 config[%s]:%s path:%s type:%s mssql:%s samba:%s" % (self.__class__.__name__, self.NAME, self.PATH_TO_BASE, self.PATH_TYPE, self.MSSQL_CONFIG, self.SMB_SERVER)
