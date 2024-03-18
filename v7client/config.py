#!/usr/bin/env python
#-*-coding:utf8-*-
import logging
import os
from datetime import timedelta, datetime
import tempfile
from dataclasses import dataclass

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


class Config:
    NAME = 'default'
    # доступ к папке с конфигурацией
    PATH_TYPE = 'dir' # smb|dir|ftp

    SMB_SERVER = ''
    SMB_SHARE  = ''
    SMB_USER = ''
    SMB_PWD = ''

    # путь к файлам базе на удаленном сервере (папка)
    PATH_TO_BASE = '/'

    FILE_1Cv7_MD = '1Cv7.MD'
    FILE_1Cv7_DDS = '1Cv7.DDS'
    FILE_1Cv7_DBA = '1Cv7.DBA'

    # update metadata interval
    UPDATE_INTERVAL = None  # timedelta(days=1)

    def __init__(self, name=None, path_type=None, 
                smb_server=None, smb_share=None, smb_user=None, smb_pwd=None, 
                sql_user=None, sql_pwd=None, sql_host=None, sql_db=None, 
                update_interval=None, path_to_base=None,
                file_1cv7_md=None, file_1cv7_dds=None, file_1cv7_dba=None):
        self.NAME = name or self.NAME
        
        
        self.SMB_SERVER = smb_server or self.SMB_SERVER
        self.SMB_PWD = smb_pwd or self.SMB_PWD
        self.SMB_SHARE = smb_share or self.SMB_SHARE
        self.SMB_USER = smb_user or self.SMB_USER

        if path_type is None and self.SMB_SERVER:
            self.PATH_TYPE = 'smb'
        else:
            self.PATH_TYPE = path_type

        self.MSSQL_CONFIG = MsSqlConfig(sql_user, sql_pwd, sql_host, sql_db)
        # self.SQL_DB = sql_db or self.SQL_DB
        # self.SQL_HOST = sql_host or self.SQL_HOST
        # self.SQL_USER = sql_user or self.SQL_USER
        # self.SQL_PWD = sql_pwd or self.SQL_PWD

        self.PATH_TO_BASE = path_to_base or self.PATH_TO_BASE
        self.FILE_1Cv7_MD = file_1cv7_md or self.FILE_1Cv7_MD
        self.FILE_1Cv7_DDS = file_1cv7_dds or self.FILE_1Cv7_DDS
        self.FILE_1Cv7_DBA = file_1cv7_dba or self.FILE_1Cv7_DBA
        
        self.UPDATE_INTERVAL = update_interval or self.UPDATE_INTERVAL

        self.prepare_paths()

    @classmethod
    def build_from_env(cls) -> 'Config':
        return cls(
            name=os.environ.get('V7_NAME', 'v7base'),
            path_type=os.environ.get('V7_PATH_TYPE', None),
            smb_server=os.environ.get('V7_SMB_SERVER', ''),
            smb_share=os.environ.get('V7_SMB_SHARE', ''),
            smb_user=os.environ.get('V7_SMB_USER', ''),
            smb_pwd=os.environ.get('V7_SMB_PWD', ''),
            sql_user=os.environ['V7_SQL_USER'],
            sql_pwd=os.environ['V7_SQL_PWD'],
            sql_host=os.environ['V7_SQL_HOST'],
            sql_db=os.environ['V7_SQL_DB'],
            path_to_base=os.environ.get('V7_PATH_TO_BASE', cls.PATH_TO_BASE),
            file_1cv7_md=os.environ.get('V7_FILE_1Cv7_MD', cls.FILE_1Cv7_MD),
            file_1cv7_dds=os.environ.get('V7_FILE_1Cv7_DDS', cls.FILE_1Cv7_DDS),
            file_1cv7_dba=os.environ.get('V7_FILE_1Cv7_DBA', cls.FILE_1Cv7_DBA),
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
