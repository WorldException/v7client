#!python
#-*-coding:utf8-*-
from __future__ import unicode_literals
'''
Created on 03.09.2010
@author: stoyanov
http://decalage.info/python/olefileio
['Metadata', 'Main MetaData Stream']
http://1c-esse.buter.ru/esse7.php?pg=1
http://1c.alterplast.ru/1cv7md/index.html
Интересный ресурс по структуре MD
http://mista.ru/articles1c/hare/article.11.html
'''

import olefile
import zlib
import os
import codecs
from .metadata import MDObject
import logging


mylog = logging.getLogger(__name__)

hex_decoder = codecs.getdecoder('hex_codec')
dump_path = os.path.join(os.path.dirname(__file__), 'dump')

#ztest = zlib.compress('//test').encode('hex')
zlib_head = hex_decoder(b'789c')[0]
zlib_tail = hex_decoder(b'0664021f')[0]
#zlib.compress('').encode('hex')[:4].decode


class MdReader:
    """
    Чтение файла MD с извлечением примитивных структур и парсингом их в списки
    """

    class ReadedConfig:
        """
        Результат чтения конфигурации, является фабрикой для парсера
        """
        def __init__(self):
            self.dds = []
            self.dialog = []
            self.entry = {}
            self.md = None

        @property
        def MdObject(self):
            if self.md is None:
                self.md = MDObject()
                self.md.parse(self.dds)
            return self.md

    def __init__(self, filename, metadata=True, dialog=False):
        self.filename = filename

        self.parse_metadata = metadata
        self.parse_dialog = dialog

        self.ole = None
        self.read_result = None
        # self.read_result = MdReader.ReadedConfig()
        self._metadata = None

    def read(self):
        mylog.info('Начинаю чтение %s' % self.filename)
        self.ole = olefile.OleFileIO(self.filename)
        read_result = MdReader.ReadedConfig()
        self.read_result = read_result
        try:
            oledirs = self.ole.listdir()
            #mylog.debug('OLE_DIRS: %s' % oledirs)
            for entry in oledirs:
                entry_name = entry[0]
                #mylog.debug('entry_name: %s' % entry_name)
                try:
                    if entry_name == 'Metadata':
                        if "Main MetaData Stream" in entry and self.parse_metadata:
                            self.handler_metadata(entry)
                    if entry_name == 'Document':
                        if "Dialog Stream" in entry and self.parse_dialog:
                            self.handler_dialog(entry)
                        if "Container.Profile" in entry:
                            continue
                        if "Container.Contents" in entry:
                            continue
                except Exception as e:
                    mylog.exception('Ошибка при чтении конфигурации %s' % str(e))
        finally:
            self.ole.close()
            self.ole = None
            self.read_result = None
        return read_result

    def handler_metadata(self, entry:str):
        if "Main MetaData Stream" in entry:
            with self.ole.openstream(entry) as f:
                tx = f.read()
            self.read_result.dds = ParseTree(tx.decode('cp1251', errors='ignore'))

    def handler_dialog(self, entry:str):
        if "Main MetaData Stream" in entry:
            with self.ole.openstream(entry) as f:
                tx = f.read()
            self.read_result.dialog = tx  # ParseTree(tx.decode('cp1251', errors='ignore'))

    @property
    def metadata(self):
        if self._metadata is None:
            result = self.read()
            self._metadata = result.MdObject
        return self._metadata


def ParseTree(text):
    # обрезка мусора до и после скобок
    p1 = text.find('{')
    if p1 < 0:
        p1 = 0
    p2 = len(text)-1
    while True:
        if p2 < 1:
            break
        if text[p2] == "}":
            p2 += 1
            break
        p2 += -1
    t = text[p1:p2]

    t = t.replace(
        "{"   , "[").replace(
        "}"   , "]").replace(
        ',"",', ",u'',").replace(
        '",'  , "',").replace(
        ',"'  , ",u'").replace(
        '""'  , '"').replace(
        '["'  , "['").replace(
        '"]'  , "']").replace(
        '\\'  , "\\\\").replace(
        '\r\n', ''
    )
    #import simplejson
    #return simplejson.loads(t)
    p = t.find("[['MainDataContDef'")
    if p > 0:
        #t = '[j'+t[p:]
        t = t[p:]
    try:
        return eval(str(t))
    except Exception as e:
        mylog.exception("Error parse MD")
        mylog.warning(t[:10])
        mylog.warning(t[-10:])
        mylog.warning('end')
    return {}


def load_md(filename):
    mylog.info('Начинаю чтение %s' % filename)
    result = MdReader(filename).read()
    mylog.info('Разбор описания метаданных')
    metadata = result.MdObject
    mylog.info('Конфигурация прочитана')
    return metadata
