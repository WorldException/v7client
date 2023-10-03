#!/usr/bin/env python
# -*-coding:utf8-*-
import logging
import re
from .metadata import FieldNotFound, MDErrorNotFound

mylog = logging.getLogger(__name__)


class PrepareError(RuntimeError):
    pass


def prepareSQL(sql, md, debug=False):
    """
    найти заготовки вида <name> <Справочник.Контрагенты> <Справочник Контрагенты.Наименование>
    """
    mylog.debug("src sql: %s" % sql)
    errors = False
    sqlres = "%s\n" % sql
    psevdo = {} # псевдонимы таблиц
    for m in re.finditer(r'(\$[#a-zа-я0-9\.^]+)\s+as\s+([a-zA-Z_0-9а-я]+)'
                         r'|(\$[#a-zа-я0-9\.^]+)\s+([a-zA-Z_0-9а-я]+)'
                         r'|(\$[#a-zа-я0-9\.^]+)',
                         sql, re.UNICODE|re.IGNORECASE|re.MULTILINE):
        #print('group(0)',m.group(0),m.group(2))
        #print(m.groups())
        ps = ''
        obj = ''
        if m.group(1):
            obj = m.group(1)
            ps = m.group(2)
        elif m.group(3):
            obj = m.group(3)
            ps = m.group(4)
        elif m.group(5):
            obj = m.group(5)
        if not obj:
            raise PrepareError('error parser groups')
        search_str = obj[1:].strip()
        field = md.get_by_path(search_str)
        sql = field.sql
        mylog.debug("Алиас: %s -> %s, псевдоним %s" % (m.group(0), sql, ps))
        if ps:
            psevdo[ps] = (m.group(0), obj, sql, field)  # запоминаю замену для псевдонима
        # вначале пытаюсь заменить значение с пробелом в конце, т.к. бывает что справочники пересекаются по именам
        if obj+' ' in sqlres:
            sqlres = sqlres.replace(obj+' ', sql+' ')
        else:
            sqlres = sqlres.replace(obj, sql)
    # а теперь вычислю значения для псевдонимов
    for p_alias, ps in psevdo.items():
        # r'[$\n\s\t,]%s.([#a-zA-Z0-9А-Яа-я_]+)]'
        # prefix psevdonim.field suffix
        reg = r'([$\n\s\t\(,=><]%s)\.([#a-zA-Z0-9А-Яа-я_]+)([\n\s\t\),=><$])' % p_alias
        for m in re.finditer(reg, sqlres, re.UNICODE|re.IGNORECASE|re.MULTILINE):
            pre = m.group(0)[0]  # символ перед псевдонимом
            sufx = m.group(3)  # последний символ за полем
            found = m.group(0)  # найденая группа целиком
            field_name = m.group(2)
            try:
                fld = ps[3].get_by_path([field_name, ])
            except FieldNotFound as e:
                sqlres = sqlres.replace(found, "%s.[NotFound %s]%s" % (m.group(1), str(e), sufx))
                errors = True
                raise FieldNotFound(found)
            except MDErrorNotFound as em:
                sqlres = sqlres.replace(found, "%s.[MDNotFound %s]%s" % (m.group(1), em.message, sufx))
                errors = True
                raise MDErrorNotFound(found)
            mylog.debug("Псевдоним: %s.%s -> %s" % (p_alias, field_name, fld.sql))
            if fld.sql[0] == '!':
                #замена всего выражения
                sqlres = sqlres.replace(found, "%s%s%s" % (pre, fld.sql[1:], sufx))
            else:
                sqlres = sqlres.replace(found, "%s.%s%s" % (m.group(1), fld.sql, sufx))
    if errors:
        mylog.error(sqlres)
        raise RuntimeError("errors prepare sql:%s" % sqlres)
    mylog.debug("RETURN: %s" % sqlres)
    return sqlres
