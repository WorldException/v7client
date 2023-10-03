#!/usr/bin/env python
#-*-coding:utf8-*-
import logging
import os
from datetime import timedelta, datetime
import tempfile
import time
import re

from .smb import PatchedSmbClient
from abc import ABC

log = logging.getLogger(__name__)


def getpath(filename=''):
    return os.path.join(os.path.dirname(__file__), filename)


class BaseConfig(ABC):
    NAME = 'default'
    # доступ к папке с конфигурацией
    PATH_TYPE = 'dir' # smb|dir|ftp

    SMB_SERVER = ''
    SMB_SHARE  = ''
    SMB_USER = ''
    SMB_PWD = ''

    # подключение к скл
    SQL_USER = 'sa'
    SQL_PWD = ''
    SQL_HOST = ''
    SQL_DB = ''

    # общий путь к папке с метаданными
    PATH_META = os.getenv('V7_PATH_META', os.path.join(tempfile.gettempdir(), 'v7_metadata'))
    # полный путь к папке метаданных 1С с учетом имени (для этой базы)
    PATH_META_DIR = ''
    # полный путь к моделям на py
    PATH_TO_MODEL = ''
    # путь к файлам базе на удаленном сервере (папка)
    PATH_TO_BASE = '/'

    FILE_1Cv7_MD = u'1Cv7.MD'
    FILE_1Cv7_DDS = u'1Cv7.DDS'
    FILE_1Cv7_DBA = u'1Cv7.DBA'
    FILES = (FILE_1Cv7_MD, FILE_1Cv7_DDS, FILE_1Cv7_DBA, '1cv7.md')
    # абсолютные пути к файлам после инициализации
    PATH_1Cv7_MD = ''
    PATH_1Cv7_DDS = ''
    PATH_1Cv7_DBA = ''

    # update metadata interval
    UPDATE_INTERVAL = None  # timedelta(days=1)

    def __init__(self):
        self.init_config()

    def METAFILE_FULL_PATH(self, filename):
        return os.path.join(self.PATH_META_DIR, filename)

    def init_config(self):
        self.PATH_META_DIR = os.path.join(self.PATH_META, self.NAME)
        self.PATH_TO_MODEL = os.path.join(self.PATH_META_DIR, 'model')
        self.PATH_1Cv7_DBA = self.METAFILE_FULL_PATH(self.FILE_1Cv7_DBA.lower())
        self.PATH_1Cv7_DDS = self.METAFILE_FULL_PATH(self.FILE_1Cv7_DDS.lower())
        self.PATH_1Cv7_MD = self.METAFILE_FULL_PATH(self.FILE_1Cv7_MD.lower())
        for dirpath in (self.PATH_META_DIR, self.PATH_TO_MODEL):
            if not os.path.exists(dirpath):
                log.info(u'Create dir for %s=%s' % (self.__class__.__name__, dirpath))
                os.makedirs(dirpath)

    def update_meta_files(self):
        #ToDo добавить лок для скачивание только одним потоком
        log.info(u'Start update meta files: %s' % self.__unicode__())
        if self.PATH_TYPE == 'smb':
            smb = self.smbclient
            
            workdir = smb.listdir(self.PATH_TO_BASE, '1*7.*')
            log.info(u'Files on smb: %s' % repr(workdir))
            found_md = False
            for filename in self.FILES:
                target_filename = filename.lower()
                
                if filename in workdir:
                    fullname = os.path.join(self.PATH_TO_BASE, filename)
                    target_name = self.METAFILE_FULL_PATH(target_filename)
                    log.info(u'downloading: %s -> %s' % (fullname, target_name))
                    smb.download(fullname, target_name)
                    if target_filename == '1cv7.md':
                        found_md = True

            if not found_md:
                log.warning('1Cv7.md not found')
                for fn in workdir:
                    log.debug(fn)
            return True

        if self.PATH_TYPE == 'dir':
            workdir = os.listdir(self.PATH_TO_BASE)
            log.debug(u'Files on smb: %s' % repr(workdir))
            for filename in self.FILES:
                target_filename = filename.lower()
                if filename in workdir:
                    fullname = os.path.join(self.PATH_TO_BASE, filename)
                    target_name = self.METAFILE_FULL_PATH(target_filename)
                    log.info(u'copying: %s -> %s' % (fullname, target_name))
                    with open(fullname, 'rb', 1024) as fsrc:
                        with open(target_name, 'wb', 1024*1024) as fout:
                            fout.write(fsrc.read())
            return True

    @property
    def smbclient(self):
        return PatchedSmbClient(server=self.SMB_SERVER, share=self.SMB_SHARE, username=self.SMB_USER, password=self.SMB_PWD)

    def download(self, remote, target, force=False) -> True:
        """
        Скачать новый файл. Проверяет файл на изменения времени создания и размера.

        :param remote: путь относительно базы на удаленном сервере
        :param target: куда сохранить
        :param force:
        :return: True если скачан более новый файл
        """
        remote_full_path = os.path.join(self.PATH_TO_BASE, remote)
        log.debug(f"remote file:{remote_full_path}")
        smb = self.smbclient
        info = smb.info(remote_full_path)
        #{'altname': 'IMPORT~1.CSV', 'create_time': 'Wed Jul 10 12:51:33 2019 MSK', 'access_time': 'Thu May  7 12:16:05 2020 MSK', 'write_time': 'Fri Nov 26 21:54:06 2021 MSK', 'change_time': 'Fri Nov 26 21:54:06 2021 MSK', 'attributes': 'NA (2020)', 'stream': '[::$DATA], 9133499 bytes'}
        try:
            remote_time = datetime.strptime(info['change_time'], '%a %b %d %H:%M:%S %p %Y %Z')
        except:
            remote_time = datetime.strptime(info['change_time'], '%a %b %d %H:%M:%S %Y %Z')
        remote_size = int(re.findall(r"(\d+)\sbytes", info['stream'], re.IGNORECASE)[0])
        log.debug(info)
        log.debug(f"change time:{remote_time}; size:{remote_size}")
        target_mtime = datetime(1,1,1)
        target_size = 0
        if os.path.exists(target):
            target_mtime = datetime.fromtimestamp(os.path.getmtime(target))
            target_size = int(os.path.getsize(target))
            log.debug(f"target time:{target_mtime}; size:{target_size}")
        if target_mtime < remote_time or target_size != remote_size or force:
            smb.download(remote_full_path, target)
            target_size = os.path.getsize(target)
            log.debug(f"target time:{target_mtime}; size:{target_size}")
            return remote_size == target_size

        return False

    def __unicode__(self):
        return u"1Cv7 config[%s]: %s path: %s type: %s" % (self.__class__.__name__, self.NAME, self.PATH_TO_BASE, self.PATH_TYPE)

    def __str__(self):
        return self.__unicode__()
