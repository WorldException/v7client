#!/usr/bin/env python
#-*-coding:utf8-*-
from __future__ import unicode_literals

"""
генератор модели sqlalchemy

посмотреть http://mrbob.readthedocs.io/en/latest/
"""
import os
import ninja
import logging
log = logging.getLogger(__name__)

template_config_class = ninja.env.get_template('sqlalchemy/config.tmpl')

template_py = """#!/usr/bin/env python
#-*-coding:utf8-*-
"""

template_database = ninja.env.get_template('sqlalchemy/database.tmpl')


model_header = '''
from database import base
from sqlalchemy import Unicode, Integer, Column

'''

model_template = '''
class %(class_name)s(base):
    ru_name = u'%(ru_name)s'
    __tablename__ = '%(tablename)s'
    id=Column('iddoc', Integer, primary_key=True)
'''

model_field = '''    %(name)s = Column('%(sql)s', %(type)s, %(options)s)
'''

import transliterate
from ..md_reader2 import MdReader
from .. import dba
import maker
import codecs


def trs(value):
    return transliterate.translit(value, 'ru', True).replace("'", '')


def _alchemy_type_field(field):
    try:
        if field._type == 'S':
            return 'String(length=%s)' % field._type_len2
        elif field._type == 'N':
            return 'Numeric(length=%s)' % field._type_len
        elif field._type == 'D':
            return 'DateTime()'
        elif field._type == 'E':
            return 'ChoiceType()'
        elif field._type == 'B':
            field_link = field.parent.parent.id_to_mdobject.get(field._type_obj)
            if not field_link is None:
                pk = field_link.primary_key
                if pk is None:
                    return ''
                else:
                    return '%s, ForeignKey("%s.%s")' % (_alchemy_type_field(pk), trs(field_link.name), pk.sql)
            return ''
    except Exception as e:
        print(e)
    return ''


ninja.env.filters['trans'] = trs
ninja.env.filters['alchemy_type'] = _alchemy_type_field

class AlchemyMaker(maker.Maker):

    def __init__(self, config):
        self.config = config

    def make(self):
        self.make_dba()
        self.make_package()
        self.make_connection()
        self.make_classes()

    def make_src(self, filename):
        full_filename = os.path.join(self.config.PATH_TO_MODEL, filename)
        log.debug(full_filename)
        f = codecs.open(full_filename, 'w', encoding='utf-8')
        #f.write(template_py)
        return f

    def append_src(self, filename):
        f = open(os.path.join(self.config.PATH_TO_MODEL, filename), 'a')
        return f

    def make_package(self):
        f = self.make_src('__init__.py')
        f.write('# init ')
        f.write('\nfrom database import DB')
        f.close()

    def make_dba(self):
        info = dba.read_dba(self.config.PATH_1Cv7_DBA, True)
        f = self.make_src('config.py')
        f.write(template_config_class.render(**info))
        f.close()

    def make_connection(self):
        f = self.make_src('database.py')
        f.write(template_database.render())
        f.close()

    def make_classes(self):
        self.md = MdReader(self.config.PATH_1Cv7_MD)
        metadata = self.md.metadata
        init_src = self.append_src("__init__.py")
        for alias, alias_type in metadata.aliases.items():
            alias_name = transliterate.slugify(alias)
            f = self.make_src('%s.py' % alias_name)
            init_src.write('\nimport %s' % alias_name)

            class_template = None
            try:
                class_template = ninja.env.get_template('sqlalchemy/%s/module.tmpl' % alias_name)
                f.write(class_template.render(items=metadata._get_by_alias(alias)))
                f.close()
                continue
            except:
                pass

            continue

            f.write(model_header)
            for item in metadata._get_by_alias(alias):

                if class_template and 1==0:
                    f.write(class_template.render(item=item))
                else:

                    # записать класс в файл
                    p = {
                        'class_name': trs(item.name),
                        'ru_name': item.name,
                        'tablename': item.sql
                    }
                    model = model_template % p
                    #f.write(model.encode('utf8'))
                    f.write(model)
                    for field in item.fields:
                        fp = {
                            'name': trs(field.name),
                            'sql': field.sql,
                            'type': 'Unicode(10)',
                            'options': ''
                        }
                        field_str = model_field % fp
                        f.write(field_str)
            f.close()
        init_src.close()
