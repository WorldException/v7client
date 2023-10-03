#!python
#-*-coding:utf8-*-

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
'''
ert
DEBUG:v7:Container.Contents
DEBUG:v7:Container.Profile
DEBUG:v7:Dialog Stream
DEBUG:v7:Inplace description
DEBUG:v7:MD Programm text
DEBUG:v7:Main MetaData Stream

Подробнее о Main Metadata Stream

Этот файл разбит на множество секций, каждая из которых взята в фигурные кавычки. Секции могут быть вложенными.
Внутри секции может находиться несколько параметров, разделенных запятыми. Секции одного вида имеют одинаковый набор параметров.
 Все секции имеют идентификаторы. Эти идентификаторы записаны в первом параметре любой секции. Если секция самого верхнего уровня,
 то она имеет текстовый идентификатор, обозначающий, какого типа метаданные описаны в этой секции. Например,
 у констант секция называется Consts, у справочников - SbCnts, у нумераторов документов - DocNumRef, и т. д.
 Если же секция содержит в себе описание какого-то конкретного объекта метаданных, то идентификатор, как правило,
 цифровой. Для тех объектов метаданных, которым можно сопоставить некий файл в базе данных, этот идентификатор
 совпадает с номером файла. Например, справочник "Номенклатура" в комплексной конфигурации имеет
 идентификатор 33 и файл sc33.dbf.
Рассмотрим строение секции на примере справочника "Номенклатура". Первым полем, как уже было сказано,
является идентификатор. Вторым - тот идентификатор, который задан в конфигураторе. Далее идут комментарий
и синоним, а так же другие параметры справочника, такие как длина кода, наименование, подчиненность,
вид основного представления, и т. д.
Затем идет секция описания параметров, называется она "Param", и вся состоит из вложенных секций-параметров.
В каждом параметре идет идентификатор, затем название в конфигураторе, комментарий с синонимом,
тип данных (U-Неопределенный, N-Число, S-Строка, D-Дата, B-Справочник, O-Документ, E-перечисление,
T-Счет, P-ПланСчетов, K-ВидСубконто, C-Календарь, A-ВидРасчета), и ряд параметров, характерных для
данного типа данных (длина, неотрицательность, периодичность, индексация, и т. д.).
И последняя секция - секция форм списков ("Form"). Здесь описаны формы списка нашего справочника.
 В данной секции так же присутствуют числовой идентификатор формы списка, идентификатор с синонимом
  и комментарием, как задано в конфигураторе. Например в моей конфигурации, для формы списка "ФормаСписка"
   идентификатор 34, а для списка "ДляПодбора" - 549.

DEBUG:v7:Page.1
DEBUG:v7:Picture
'''
'''
md
DEBUG:v7:AccountChart
DEBUG:v7:AccountChartList
DEBUG:v7:CalcJournal
DEBUG:v7:CalcVar
DEBUG:v7:Container.Contents
DEBUG:v7:Document
DEBUG:v7:GlobalData
DEBUG:v7:Journal
DEBUG:v7:Metadata   [['MainDataContDef', '4701', '10011', '7120'], ['TaskItem',
DEBUG:v7:Operation
DEBUG:v7:OperationList
DEBUG:v7:Picture
DEBUG:v7:ProvList
DEBUG:v7:Report
DEBUG:v7:SubFolder
DEBUG:v7:SubList
DEBUG:v7:Subconto
DEBUG:v7:TypedText
DEBUG:v7:UserDef

AccountChart
    Тут лежат экранные формы счетов. Поскольку форма счета в конфигурации может быть только одна, то и поддиректория здесь тоже только одна. Всегда.
AccountChartList
    Тут лежат экранные формы списка планов счетов.
CalcJournal
    Экранные формы журналов расчетов.
CalcVar
    Обработки.
Document
    Документы. Тут все, кроме модуля проведения документа.
GlobalData
    Общие таблицы.
Journal
    Формы журналов документов.
Metadata
    См. выше.
Operation
    Экранная форма бухгалтерской операции.
OperationList
    Экранные формы журналов операций.
Picture
    Галерея картинок конфигурации.
ProvList
    Экранные формы журналов проводок.
Report
    Отчеты.
Subconto
    Экранные формы элементов справочников.
SubFolder
    Экранные формы групп справочников.
SubList
    Экранные формы форм списков справочников.
TypedText
    Эта директория сильно отличается от предыдущих. Тут находятся: Модули типов расчетов (каталоги CalcArg_Number*), Глобальный модуль (каталог ModuleText_Number1) Модули проведения документов (каталоги Transact_Number*), а так же описания всех объектов метаданных (каталоги UserHelp_Number*).
UserDef
    Тут хранятся описания интерфейсов (папка Page.1) и наборы прав пользователей (папка Page.2). Формат хранения человеконечитаемый, пользуемся
'''
DUMP_META = False

#import OleFileIO_PL
import olefile
import zlib
import os
import codecs
import logging
from typing import TypedDict, Any

mylog = logging.getLogger(__name__)

hex_decoder = codecs.getdecoder('hex_codec')

dump_path = os.path.join(os.path.dirname(__file__),'dump')
ztest = zlib.compress(b'//test').hex()
#zlib_head = hex_decoder(b'789c')[0]
#zlib_tail = hex_decoder(b'0664021f')[0]
zlib_head = b'789c'
zlib_tail = b'0664021f'
#zlib.compress('').encode('hex')[:4].decode
#

def ParseTree(text:str) -> dict[str, Any]:
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
        "{"    , "[").replace(
        "}"    , "]").replace(
        ',"",' , ",u'',").replace(
        '",'   , "',").replace(
        ',"'   , ",u'").replace(
        '""'   , '"').replace(
        '["'   , "['").replace(
        '"]'   , "']").replace(
        '\\'   , "\\\\").replace(
        '\r\n' , '')
    p = t.find("[['MainDataContDef'")
    if p>0:
        t = t[p:]
    try:
        return eval(str(t))
    except Exception as e:
        mylog.exception("Error parse MD")
        mylog.warning(t[:10])
        mylog.warning(t[-10:])
        mylog.warning('end')
    return {}

def dump_stream(name, data):
    with open(os.path.join(dump_path, name), 'w+') as f:
        f.write(data)

class MetadataDict(TypedDict):
    dds: dict
    entry: dict


def parse_md(filename) -> MetadataDict:
    mylog.info('Начинаю чтение %s' % filename)
    
    m: MetadataDict = MetadataDict(dds=dict(), entry=dict())
    
    ole = olefile.OleFileIO(filename)
    # mylog.debug('OLE_DIRS: %s' % ole.listdir())
    
    for entry in ole.listdir():
        mylog.debug(entry[0])

        #with open("stream_%s" % entry[0],'w+') as f:
        #    f.write(repr(entry))

        if entry[0]=='Document':
            #print(entry)
            if "Dialog Stream" in entry:
                continue
                try:
                    sz= ole.get_size(entry)
                    f=ole.openstream(entry)
                    #print(f.read(sz))
                    f.close()
                except Exception as e:
                    mylog.exception(repr(e.args))
            if "Container.Profile" in entry:
                continue
                try:
                    sz= ole.get_size(entry)
                    f=ole.openstream(entry)
                    #print(f.read(sz))
                    f.close()
                except:
                    mylog.exception()
            if "Container.Contents" in entry:
                continue
                sz= ole.get_size(entry)
                f=ole.openstream(entry)
                #print(f.read(sz))
                f.close()

            if "MD Programm text" in entry:
                continue
                '''
                Пока что не работает, работало в прежних версиях python
                try:
                    sz= ole.get_size(entry)
                    f=ole.openstream(entry)
                    tx= f.read(sz)
                    f.close()
                    #print(zlib.compress('test').encode('hex'))
                    zi=zlib.decompress((zlib_head+tx))
                    print(zi)
                except Exception,e:
                    mylog.exception('read MD Programm text')
                    mylog.info(tx[:10].encode('hex'))
                    #print(e)
                '''
        if entry[0]=='Metadata':
            if "Main MetaData Stream" in entry:

                try:
                    #sz= ole.get_size(entry)
                    f = ole.openstream(entry)
                    tx = f.read()
                    f.close()
                    #print(zlib.compress('test').encode('hex'))
                    #d=zlib.decompressobj()
                    #zi=zlib.decompress(zlib_head+tx)
                    #tx_fixed = utils.fixunicode(tx,'cp1251')
                    #mylog.debug(tx.decode('cp1251'))
                    m['dds'] = ParseTree(tx.decode('cp1251', errors='ignore'))
                except Exception as e:
                    mylog.exception('parse metadata error')
        #if entry[0] == 'Journal':
        #write dumps
        if DUMP_META:
            if "MD Programm text" in entry:
                sz= ole.get_size(entry)
                f=ole.openstream(entry)
                tx= f.read(sz)
                f.close()
                hx = tx.hex()
                if ztest.find(hx) > 0:
                    #print(entry)
                    #print(hx)
                    pass
                try:
                    #zlib.compress("//test").encode('hex')
                    #'789c d3d72f492d2e0100 0664021f'
                    #      d3d72f492d2e0100
                    tx=zlib.decompress(zlib_head+tx)
                    #print("MODULE:", tx)
                    pass
                except Exception as e:
                    #print("size MD text:", sz, str(e))
                    pass
                dump_stream("entry-%s" % entry, zlib_head+tx)
            else:
                dump_stream("entry-%s" % entry, ole.openstream(entry).read())
    return m

from .metadata import MDObject

def extract_metadata(meta):
    '''
    извлекает все структуры
    :param m:
    :return:
    '''
    md = MDObject()
    md.parse(meta['dds'])
    return md

def load_md(filename):
    mylog.info('Начинаю чтение %s' % filename)
    metadata = MDObject()
    meta = parse_md(filename)
    mylog.info('Разбор описания метаданных')
    metadata.parse(meta['dds'])
    mylog.info('Конфигурация прочитана')
    return metadata
