#!/usr/bin/env python
#-*-coding:utf8-*-
from __future__ import unicode_literals

import logging
import transliterate
from ..md_reader2 import MdReader
from ..metadata import FC_TABLE
from .. import dba
from . import maker

log = logging.getLogger(__name__)
import simplejson, yaml


def _type_none(fld):
    return {}

def _type_r1(fld):
    return dict(length=fld._type_len)

def _type_link(fld):
    field_link = fld.parent.parent.id_to_mdobject.get(fld._type_obj)
    if not field_link is None:
        return dict(objid=fld._type_obj, name=field_link._name)
    return dict(objid=fld._type_obj)

_type_names = {
    'S': ('Строка', _type_r1),
    'D': ('Дата', _type_none),
    'E': ('Перечисление', _type_link),
    'B': ('Ссылка', _type_link),
    'N': ('Число', _type_r1),
}


def field_as_dict(fld):
    type_repr = _type_names.get(fld._type, (fld._type, _type_none))
    return dict(name=fld.name, sql=fld.sql, type={type_repr[0]: type_repr[1](fld)})


def object_as_dict(obj):
    flds = {}
    ret = {'name': obj.name, 'table': obj.sql, 'Поля': flds}
    flds['ТабличнаяЧасть'] = {}
    for f in obj.fields:
        fld_dict = field_as_dict(f)
        if f._name == 'ТабличнаяЧасть':
            continue
        if f._place == FC_TABLE:
            flds['ТабличнаяЧасть'][fld_dict.pop('name')] = fld_dict
        else:
            flds[fld_dict.pop('name')] = fld_dict
    return ret


class DictMaker(maker.Maker):

    def make_classes(self):
        self.md = MdReader(self.config.PATH_1Cv7_MD)
        metadata = self.md.metadata
        result = {}

        for alias, alias_type in metadata.aliases.items():
            alias_name = transliterate.slugify(alias)
            log.debug(alias_type.ru_name)

            for item in metadata._get_by_alias(alias):
                # записать класс в файл

                #print(simplejson.dumps(item.as_dict()))
                if not alias_type.ru_name in result:
                    result[alias_type.ru_name] = {}

                obj_dict = object_as_dict(item)
                result[alias_type.ru_name][obj_dict.pop('name')] = obj_dict

        meta = self.make_src('metadata.yaml')
        yaml_repr = yaml.safe_dump(result, default_flow_style=False, encoding='utf-8', allow_unicode=True)
        meta.write(yaml_repr.decode('utf8'))
        meta.close()

        meta = self.make_src('metadata.json')
        yaml_json = simplejson.dumps(result, indent=4)
        meta.write(yaml_json)
        meta.close()

