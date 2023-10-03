#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from unittest import TestCase
from v7client import md_reader, metadata
from data import file_md, file_ert, get_data_file
import json
import tempfile
import pprint

class TestMdExtractor_md(TestCase):

    def test_parse_md(self):
        r = md_reader.parse_md(file_md)
        #print(r['dds'])
        dds = r['dds']
        for m in dds:
            print(m[0])
            if m[0]=='Documents':
                print(m)
                docs = m[1:]
                for doc in docs:
                    if doc[0] == '4060':
                        print(doc)
                        for info in doc:
                            print(info)
                        #doc = md_reader.extract_Document(doc)
                        #pprint.pprint(doc)
            if m[0]=='Catalog':
                print(m)
                docs = m[1:]
                for doc in docs:
                    if doc[0]=='4060':
                        print(doc)
                        for info in doc:
                            print(info)
                        #doc = md_reader.extract_Document(doc)
                        #pprint.pprint(doc)
        print(r['dds'][1][1][1])
        js = json.dumps(r['dds'],indent=2)
        filename = tempfile.mktemp('.json', 'md_')
        print('dds export: '+filename)
        json.dump(r['dds'], open(filename, 'w+'), indent=2)


    def test_parse_ert(self):
        r = md_reader.parse_md(file_ert)

    def test_extractor(self):
        r = md_reader.parse_md(file_md)
        m = md_reader.extract_metadata(r)
        d = m.x(u"Документ.АктПередачиТМЦ")
        pprint.pprint(d)
        s = m.x(u"Справочник.Автомобили")
        pprint.pprint(s)
        print(str(s))
        r = m.x(u"Регистр.ОстаткиТоваров")
        print(u"ОстаткиТоваров", str(r))

    def test_path_find(self):
        r = md_reader.parse_md(file_md)
        m = md_reader.extract_metadata(r)
        print("test 1", m.keys())
        s1 = m[0]

        print(type(s1.name), s1.name)
        print('s1:',metadata.oprint(s1))
        print("test 2")
        s2 = m.get_by_path(u'справочник.Автомобили.Цвет')
        print(metadata.oprint(s2))
        print('test 3')
        print(u"%s" % m.get_by_path(u'справочник.Автомобили.пробег'))
        p = u"Пробег"
        print(p, p.lower(), p==p.lower(), type(p), type(p.lower()))


    def test_Field(self):
        f = metadata.FieldObject()
        print(f)

    def test_read_zip_text(self):
        fn = get_data_file('test.zip')

        import gzip
        f = gzip.open(fn)
        try:
            print(f.read())
        finally:
            f.close()

    def test_unicode_eval(self):
        f = open('unicode_err.txt','r')
        d = f.read()
        f.close()
        print(d)
        import string
        cnt_start = sum([1 if i=='[' else 0 for i in d])
        cnt_end = sum([1 if i==']' else 0 for i in d])
        print("count [:%s, ]:%s" % (cnt_start, cnt_end))
        d = d.replace('%', '').replace('\\',"\\\\")

        print(eval(d))
