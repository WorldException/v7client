#!/usr/bin/env python
# -*-coding:utf8-*-
from unittest import TestCase
from v7client.query_translator import prepareSQL
from data import read_md
import logging
mylog = logging.getLogger(__name__)

class TestPrepareSQL(TestCase):

    def setUp(self):
        self.md = read_md()

    def test_prepareSQL(self):
        print(prepareSQL(u"select avto.Наименование, avto.Код from $Справочник.Автомобили avto", self.md))
        print(prepareSQL(u"select * from $Документ.ИзменениеДопЦен.#Товар ", self.md))
        print(prepareSQL(u"select d.#Товар * from $Документ.ИзменениеДопЦен as d ", self.md))
        print(prepareSQL(u"select d.#Товар from $Документ.ИзменениеДопЦен d ", self.md))
        print(prepareSQL(u"select d.#Товар from $Документ.#ИзменениеДопЦен d ", self.md))
        print(prepareSQL(u"select ($Перечисление.СтатусыЗаказа.case) and ($Перечисление.СтатусыЗаказа.#СозданЗаказКлиента)", self.md))
