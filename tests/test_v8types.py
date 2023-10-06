#!/usr/bin/env python
# -*-coding:utf8-*-
from __future__ import unicode_literals, print_function
from unittest import TestCase
from v7types import *


class TestV8Table(TestCase):

    def test_head(self):
        v8 = V8Table(['test1', 'test2', 'test3'])
        print(v8.__v8_makeheader__())

    def test_lines(self):
        v8 = V8Table(['test1', 'test2', 'test3'])
        print(v8.__v8_makeheader__())
        v8.add(['102', '2323', 'test'])
        v8.add(['1099', '000', 'asd'])
        print(v8.lines)

    def test_all(self):
        v = V8Table(['test1', 'test2', 'test3'])
        v.add(['000', '000', '000"test"'])
        # v8.add('1099','000','asd')
        print(v.export().encode('cp1251'))

    def test_vallist(self):
        v = V8ValueList()
        v.add('value1', 'name1')
        print(v.export())

    def test_array(self):
        ar = V8Array()
        ar.append('123123')
        print('array', ar.export())

    def test_add(self):
        self.fail()

    def test_append_all(self):
        self.fail()

    def test_ncols(self):
        self.fail()

    def test_save(self):
        self.fail()

    def test_export(self):
        self.fail()
