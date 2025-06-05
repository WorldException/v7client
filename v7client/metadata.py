#!/usr/bin/env python
# -*-coding:utf8-*-
# полезное по прямым запросам
# http://www.1cpp.ru/forum/YaBB.pl?num=1148038411
# описание таблиц
# http://www.script-coding.com/v77tables.html
import logging
from v7client import utils

mylog = logging.getLogger(__name__)

# Тип данных
FT_TEXT  = 'C'
FT_FLOAT = 'N'
FT_INT   = 'I'
FT_DATA  = 'D'

# тип поля
FC_ATTR  = 'A'  # атрибут
FC_HEAD  = 'H'  # реквизит шапки
FC_TABLE = 'T'  # реквизит табличного поля
FC_DT    = 'DT' # табличное поле
FC_ID    = 'ID' # ID в значении

# маски для формирования SQL представлений согласно типу поля
field_sql_mask = {
    FC_ATTR:  '%s',
    FC_HEAD:  'SP%s',
    FC_TABLE: 'SP%s',
    FC_DT:    "DT%s",
    FC_ID:    '%s'
}


def oprint(obj):
    r= f"name:{obj.name}  sql:{obj.sql}"
    r += 'FIELDS:\n'
    for f in obj.fields:
        r += f"{f}\n"
    return r


class FieldNotFound(RuntimeError):
    pass


class MetaObject(dict):
    """
    Базовый класс для всех остальных обьектов метаданных
    parser - структура определяющая какому атрибуту какое поле присвоить из входящей структуры из MD
    meta - это соотвествия для корневого списка
    пример структуры
    [2901, 'ИмяОбъекта', "", "Описание или синоним, ['Fields', [342, 'Реквизит', ...],..]
    """
    # название типа
    ru_name = "Документ"
    # Имя в md файле
    md_name = "Documents"
    # настройки парсера полей
    parser = {
        'meta': {
            '_id': 0,
            '_sql': 0,  # поле _sql по индексу 0
            '_name': 1,
            '_description': 3,
        }
    }
    #
    field_prefix = ''
    table_prefix = ''

    print_metadata = False

    # спец. тип вроде журнала, констант и т.д. имеющий одно представление (одну таблицу)
    special = False

    # имя поля с первичным ключем
    key = 'ID'

    def __init__(self, name='', **kwargs):
        self._sql = ''
        self._name = name
        self._description = ''

        # список полей в базе данных, как шапки так и табличных
        self.fields = []
        self.need_table = False
        # ссылка на корень метаданных
        self.parent = None
        self.md = None
        self._id = ''
        self.__dict__.update(**kwargs)

    def _find_field(self, name, place=[FC_HEAD, FC_ATTR]):
        """
        Поиск реквизита по наименованию и типу
        :param name: имя реквизита как есть
        :param place: тип поля
        :return: обьект поля
        """
        if name.startswith("#"):
            place = FC_TABLE
            name = name[1:]
        _name = name.lower()
        for f in self.fields:
            if place and type(f) is FieldObject and f._place not in place:
                continue
            if f.name.lower() == _name:
                return f
        raise FieldNotFound(_name)

    def addField(self, f):
        """
        :rtype: FieldObject
        """
        f.parent = self
        self.fields.append(f)

    @property
    def sql(self):
        """
        :return: возращает текстовое представление в бд
        """
        pref = ""
        if (not self.parent is None) and len(self.parent.dbo_name)>0:
            pref = "%s." % self.parent.dbo_name
        if self.need_table:
            return "%s%s%s" % (pref, self.table_prefix, self._sql)
        else:
            return "%s%s%s" % (pref, self.field_prefix, self._sql)

    @property
    def name(self):
        return self._name

    def __str__(self):
        return 'Obj:%s [%s]\n%s' % (self.ru_name, self.md_name, oprint(self))

    def get_by_path(self, path):
        """
        Получить рекурсивно объект по пути
        :param path: [документ,поступление,датадок]
        :return: найденый обьект
        """
        if len(path) == 0:
            return self
        # if type(path) in (str, basestring, unicode):
        #     path = path.lower().split('.')
        key = path.pop()
        return self._find_field(key).get_by_path(path)

    def after_parse(self):
        # действия по завершению создания парсингом, для перекрытия в наследниках
        # Номер метаданных в конфигурации, это же вид документа, он же номер в имени таблице.
        self.addField(FieldObject(name='MdID', sql="%s" % self._sql, place=FC_ATTR))

    @classmethod
    def print_metadata_info(cls, m, level=0):
        if cls.print_metadata:
            if level == 0:
                mylog.debug("Metadata %s, %s" % (cls.ru_name, cls.md_name))
            for i in m:
                mylog.debug('>%s%s' % ('>'*level, i))
                if type(i) == list:
                    cls.print_metadata_info(i, level+1)

    @classmethod
    def parse(cls, m, parser=None, place=FC_HEAD):
        """
        Выполняет разбор структуры md файла в набор обьектов
        Каждый класс может по своему парсить свой набор,но это подходит для многих
        :param m:
        :return:
        """
        cls.print_metadata_info(m)
        # сначала беру основные атрибуты
        doc = cls()
        for attr_name, index in cls.parser['meta'].items():
            #doc.__setattr__(attr_name, m[index])
            setattr(doc, attr_name, m[index])
        # теперь разбираю информацию о полях
        for i in m:
            # для вложенных полей ищу сопадающие правила
            if type(i) == list:
                if i[0] in cls.parser:
                    fieldparser = cls.parser[i[0]]
                    for h in i[1:]:
                        f = fieldparser(h)
                        f.parent = doc
                        doc.addField(f)
        doc.after_parse()
        return doc

    def as_dict(self):
        ret = dict(name=self.name, sql=self.sql, fields=[])
        flds = ret['fields']
        for f in self.fields:
            flds.append(f.as_dict())
        return ret

    @property
    def primary_key(self):
        f = self._find_field(self.key)
        return f


class FieldObject(MetaObject):

    meta_type = dict(
        _type=4,
        _type_len=5,
        _type_len2=6,
        _type_obj=7,
    )

    def __init__(self, name='', sql='', place=FC_HEAD, **kwargs):
        MetaObject.__init__(self, name, **kwargs)
        self._place = place
        self._name = name
        self._sql = sql
        self._type = None
        self._type_len = None
        self._type_len2 = None
        self._type_obj = None
        for attr_name, index in self.meta_type.items():
            if not hasattr(self, attr_name):
                setattr(self, attr_name, '')

    def __str__(self):
        return f'Field[{self._place}]:{self.name}, {self.sql} type:{self._type}:{self._type_len}.{self._type_len2}'

    @classmethod
    def parse(cls, m, parser, place=FC_HEAD):
        f = FieldObject(place=place)
        for attr_name, index in parser.items():
            setattr(f, attr_name, m[index])
        if len(m) > 7:
            for attr_name, index in cls.meta_type.items():
                setattr(f, attr_name, m[index])
        return f

    @property
    def sql(self):
        if isinstance(self.parent, EnumObject) and self._place == FC_TABLE:
            # если это поле перечисления и указан признак таблицы, то верну ID поля
            return "'%s'" % utils.ID_36(self._sql)
        return field_sql_mask[self._place] % self._sql

    def as_dict(self):
        return dict(name=self.name, sql=self.sql)

# обработчики для создания парсеров поименно
def field_parser(rulles, place):
    def parser(m):
        return FieldObject.parse(m, rulles, place)
    return parser

def object_parser(cls):
    def parser(m):
        return cls.parse(m)
    return parser


class DocumentObject(MetaObject):
    ru_name = "Документ"
    md_name = "Documents"
    parser = {
        'meta': {
            '_sql': 0,
            '_name': 1,
            '_description': 3,
        },
        'Head Fields': field_parser({'_sql': 0, '_name': 1}, FC_HEAD),
        'Table Fields': field_parser({'_sql': 0, '_name': 1}, FC_TABLE)
    }
    field_prefix = 'DH'
    table_prefix = 'DT'

    #print_metadata = True
    key = 'Код'
    global_refs:list[FieldObject] = []

    def __init__(self):
        MetaObject.__init__(self, '')

    def after_parse(self):
        # добавляю дополнительные поля
        self.addField(FieldObject(name="ТабличнаяЧасть", sql=self._sql, place=FC_ATTR))
        self.addField(FieldObject(name="Код", sql="idDoc", place=FC_ATTR, _type='S', _type_len='10'))
        self.addField(FieldObject(name="iddoc", sql="idDoc", place=FC_ATTR, _type='S', _type_len='10'))
        self.addField(FieldObject(name="НомерСтроки", sql="LINENO_", place=FC_ATTR, _type='N'))
        self.addField(FieldObject(name='ВидДокумента', sql="!%s" % self._sql, place=FC_ATTR))
        self.addField(FieldObject(name="НомерСтроки", sql="LINENO_", place=FC_TABLE, _type='N'))
        #self.addField(FieldObject(name="Код", sql="code", place=FC_ATTR))
        # добавляю общие реквизиты
        for ref in self.global_refs:
            self.addField(ref)
        super(DocumentObject, self).after_parse()


class AllDocumentFieldObject(MetaObject):
    """
    Общие реквизиты документов
    могут хранитьв журнале и в таблице документов
    """
    ru_name = "ОбщийРеквизит"
    md_name = "GenJrnlFldDef"
    #instance = None

    def __init__(self, name=''):
        MetaObject.__init__(self, name)
        #self._name = 'ОбщийРеквизит'

    parser = {
        'meta': {
            '_sql': 0,
            '_name': 1,
            '_description': 2,
            '_in_journ': 10  # 1 реквизит с отбором, значит в журнале, иначе в табл. документа
        },
    }
    #print_metadata = True

    @property
    def sql(self):
        if self._in_journ:
            return 'SP%s' % self._sql
        else:
            return '!!SeeDocumentTable!!'

    """
    @classmethod
    def parse(cls, m):
        cls.print_metadata_info(m)
        if not cls.instance:
            cls.instance = cls()
        doc = cls.instance
        f = FieldObject.parse(m, cls.parser['meta'], FC_ATTR)
        f.parent = doc
        doc.addField(f)
        return doc
    """


class CatalogObject(MetaObject):
    ru_name = "Справочник"
    md_name = "SbCnts"
    parser = {
        'meta':{
            '_sql': 0,
            '_name': 1,
            '_description': 2,
        },
        'Params': field_parser({
            '_sql': 0,
            '_name': 1
        },FC_HEAD),
    }
    field_prefix = 'SC'
    table_prefix = 'SC'

    def __init__(self):
        MetaObject.__init__(self)

    def after_parse(self):
        self.addField(FieldObject(name="ID", sql="id", place=FC_ATTR, _type='S', _type_len2=9))
        self.addField(FieldObject(name="Код", sql="code", place=FC_ATTR))
        self.addField(FieldObject(name="Наименование", sql="descr", place=FC_ATTR))
        self.addField(FieldObject(name="ПометкаНаУдаление", sql="ismark", place=FC_ATTR))
        self.addField(FieldObject(name="ЭтоГруппа", sql="isFolder", place=FC_ATTR))
        self.addField(FieldObject(name="Родитель", sql="parentid", place=FC_ATTR))
        self.addField(FieldObject(name="Владелец", sql="parentext", place=FC_ATTR))
        super(CatalogObject, self).after_parse()


class RegisterObject(MetaObject):
    ru_name = "Регистр"
    md_name = "Registers"
    parser = {
        'meta': MetaObject.parser['meta'],
        'Props': field_parser({
            '_sql': 0,
            '_name': 1
        }, FC_HEAD),
        'Figures': field_parser({
            '_sql': 0,
            '_name': 1
        }, FC_HEAD),
        'Flds': field_parser({
            '_sql': 0,
            '_name': 1
        }, FC_HEAD),
    }
    field_prefix = 'RG'
    table_prefix = 'RA'

    def after_parse(self):
        self.addField(FieldObject(name="period", sql="period", place=FC_ATTR, _type='D'))
        self.addField(FieldObject(name="iddoc", sql="iddoc", place=FC_ATTR))
        self.addField(FieldObject(name="НомерСтроки", sql="lineno_", place=FC_ATTR))
        self.addField(FieldObject(name="actno", sql="actno", place=FC_ATTR))
        self.addField(FieldObject(name="debkred", sql="debkred", place=FC_ATTR))
        self.addField(FieldObject(name="iddocdef", sql="iddocdef", place=FC_ATTR))
        self.addField(FieldObject(name="date_time_iddoc", sql="date_time_iddoc", place=FC_ATTR))
        super(RegisterObject, self).after_parse()


class EnumObject(MetaObject):
    """
    $Перечисление.ЧтоТо.#ЗначениеПеречисления -> '   id  '
    """
    ru_name = 'Перечисление'
    md_name='EnumList'
    parser = {
        'meta': MetaObject.parser['meta'],
        'EnumVal': field_parser({'_sql': 0, '_name': 1}, FC_TABLE),
    }

    def after_parse(self):
        # добавить спец поля для представления списком и т.д case 123 $перечисление.чегото.case Name
        sql_to_name_case = ''
        for item in self.fields:
            id = utils.ID_36(item._sql)
            sql_to_name_case += "\n when '%s' then '%s'" % (id, item._name)
        sql_to_name_case += "\nelse '' end\n"
        self.addField(FieldObject('case', sql_to_name_case, FC_ATTR))

    @property
    def sql(self):
        return '%s %s' % (self._sql, self._name)


class JournObject(MetaObject):
    """
    создается вручную без парсера
    """
    ru_name = 'Журнал'
    special = True

    global_refs = []

    def __init__(self, name=''):
        MetaObject.__init__(self, name)
        self._name = 'Журнал'
        self._sql = '_1SJourn'

        self.addField(FieldObject('ROW_ID', 'ROW_ID', FC_ATTR))
        # [int] – идентификатор журнала, ID объекта класса Journalisters, значение в 36/64-ричной системе
        # (в зависимости от версии V7). Тип – Char(4). Определяется реквизитом Documents.Jrnl_ID.
        # Фактически 1SJourn – это «полный журнал».
        self.addField(FieldObject('IDJOURNAL', 'IDJOURNAL', FC_ATTR))
        # [int] – PKey документа, такой же как в DHxxx и DTxxx. Тип – Char(9). IDDOC всех документов хранятся в 1SJourn.
        self.addField(FieldObject('IdDoc', 'IDDOC', FC_ATTR))
        # [int] – ссылка на ID объекта-таблицы Documents.ID. Тип – Char(4).
        self.addField(FieldObject('ВидДокумента', 'IDDocDef', FC_ATTR))
        # возможность документа работать с компонентами: оперативный учет (1), расчет (2), бухучет (4) – и их комбинация.
        # Для документа «Операция» APPCODE=20. Тип – Numeric(3,0). Возможно, существуют и другие значения.
        # Возможность работы с различными компонентами определяется значениями Documents.PrTrade, Documents.PrSalary, Documents.PrAccount.
        self.addField(FieldObject('AppCode', 'APPCode', FC_ATTR))
        # дата документа. Тип – Date(8).
        self.addField(FieldObject('ДатаДок', 'DATE_TIME_IDDOC', FC_ATTR))
        #self.addField(FieldObject('ДатаДок_Дата', 'cast(left(j.ДатаДок, 8) as DateTime)'))
        self.addField(FieldObject('НомерДок', 'DocNo', FC_ATTR))
        #  флаг закрытия/проведения документа. Тип – Numeric(1,0).
        self.addField(FieldObject('Проведен', 'CLOSED', FC_ATTR))
        self.addField(FieldObject('ПометкаНаУдаление', 'ISMark', FC_ATTR))
        # счетчик действий (движения) для данного документа (один документ может вызывать несколько движений регистра).
        # Похоже, что связан с полями ACTNO таблиц RAxxx и 1SCONST. Тип – Numeric(6,0).
        self.addField(FieldObject('ACTCNT', 'ACTCNT', FC_ATTR))
        # добавляю общие реквизиты
        for ref in self.global_refs:
            self.addField(ref)


class MDErrorNotFound(RuntimeError):
    pass


class MDObject(dict):
    """
    Метаданные конфигурации
    """
    parser = {
        'Documents': DocumentObject,
        'SbCnts': CatalogObject,
        'Registers': RegisterObject,
        AllDocumentFieldObject.md_name: AllDocumentFieldObject,
        EnumObject.md_name: EnumObject,
    }

    def __init__(self, **kwargs):
        self.name = ''
        # имя которое будет добавлено к таблицам work.dbo
        self.dbo_name = ''
        for key, value in kwargs:
            setattr(self, key, value)

        self.objects = []     # это любое из метаданных
        self.aliases = {
            'документ': DocumentObject,
            'справочник': CatalogObject,
            'регистр': RegisterObject,
            'перечисление': EnumObject,
            AllDocumentFieldObject.ru_name.lower(): AllDocumentFieldObject,
            JournObject.ru_name.lower(): JournObject
        }

        self.sql_to_field_index = {}
        # индекс по id
        self.id_to_mdobject = {}

    def _update_sql_to_field_index(self):
        """ обновляет индекс соответствия имени поля в базе данных и обьект Поле"""
        self.sql_to_field_index.clear()
        for item in self.objects:
            self.sql_to_field_index[item.sql] = item
            for field in item.fields:
                self.sql_to_field_index[field.sql] = field

    def getFieldNameBySQL(self, sqlname):
        name = sqlname.upper()
        if name in self.sql_to_field_index:
            return self.sql_to_field_index[name].name
        return sqlname

    def _find_by_name(self, obj_list, name):
        # найти в списке элементов нужный вид
        name_ = name.lower()
        mylog.debug('__find_by_name__(%s)' % name_)
        for item in obj_list:
            if item.special:
                return item.get_by_path([name_,])
            if item.name.lower() == name_:
                return item
        raise MDErrorNotFound(name_)

    def _get_by_alias(self, alias):
        # создает итератор по алиасу
        al = alias.lower()
        found = False
        mylog.debug('get_by_alias(%s)' % al)
        if al in self.aliases:
            cls = self.aliases[al]
            for item in self.objects:
                if type(item) is cls:
                    found = True
                    #mylog.debug('found %s' % unicode(item))
                    yield item
        if not found:
            raise MDErrorNotFound(al)

    @property
    def x_catalogs(self):
        """
        Справочники
        :return:
        """
        return self._get_by_alias('справочник')

    def get_by_path(self, path):
        """
        Получить обьект по имени Справочник.Автомобили.Модель
        :param path:
        :return:
        """
        # сначала по типу объекта ищу список доступных классов, походящих под тип
        if type(path) is str:
            path = path.lower().split('.')
        if len(path) == 0:
            return None
        path.reverse()
        #mylog.info('%s' % repr(path))
        alias = path.pop()
        # сейчас путь может начинаться только с двух слов
        obj = self._get_by_alias(alias)

        if len(path) == 0:
            # возращаю только спец объекты
            for o in obj:
                if o.special:
                    return o

        name = path.pop()

        # теперь ищу нужный вид заданного типа == значению после .
        mylog.debug('Поиск второй части %s' % name.replace('#', ''))
        o = self._find_by_name(obj, name.replace('#', ''))
        o.need_table = True if name.startswith("#") else False

        return o.get_by_path(path)

    def x(self, path):
        return self.get_by_path(path)

    def parse(self, dds):
        """
        Разбор полученной структуры происходит здесь
        :param dds: 
        :return: 
        """

        parsers = self.parser.keys()
        for m in dds:
            typeGr = m[0]
            if typeGr == 'TaskItem':
                self.name = m[1][1]
                self.name_alias = m[1][3]
            elif typeGr == 'GenJrnlFldDef':
                # общие реквизиты
                for item in m[1:]:
                    fld_obj = FieldObject(name=item[1], sql='SP%s' % item[0], place=FC_ATTR, _type=item[4], _type_len=item[5])
                    if int(item[10]):
                        # для журнала
                        JournObject.global_refs.append(fld_obj)
                    else:
                        DocumentObject.global_refs.append(fld_obj)
            elif typeGr in parsers:
                cls = self.parser[typeGr]
                for item in m[1:]:
                    obj = cls.parse(item)
                    obj.parent = self
                    if obj._sql:
                        self.id_to_mdobject[obj._sql] = obj
                    else:
                        pass
                    self.objects.append(obj)
            else:
                mylog.debug('Нет парсера для %s' % typeGr)
        self.objects.append(JournObject())
        self._update_sql_to_field_index()

    def __str__(self):
        s = []
        for item in self.objects:
            s.append(str(item))
        return '\n'.join(s)
