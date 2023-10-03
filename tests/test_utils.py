#!/usr/bin/env python
# -*-coding:utf8-*-
from unittest import TestCase
from v7client import utils


ustr = "\u041a\u043e\u043c\u0430\u043d\u0434\u0430\u0412\u044b\u0433\u0440\u0443\u0437\u043a\u0438\u041e\u0441\u0442\u0430\u0442\u043a\u043e\u0432"


class TestUtils(TestCase):

    def test_unicode_reader(self):
        r = utils.fixunicode("\u041a\u043e\u043c\u0430\u043d\u0434\u0430\u0412\u044b\u0433\u0440\u0443\u0437\u043a\u0438\u041e\u0441\u0442\u0430\u0442\u043a\u043e\u0432",'')
        assert r == u"КомандаВыгрузкиОстатков"
        print(r, type(r))
        print(utils.fixunicode('test 123123'))

    def test_unicode_decode(self):
        print(ustr.decode('unicode_escape'))

    def test_u36(self):
        self.assertEqual(utils.ID_36(u'3332'), '   2KK   ')
        self.assertEqual(utils.ID_36(3332), '   2KK   ')
        self.assertEqual(utils.ID_36('3332'), '   2KK   ')
