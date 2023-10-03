#!/usr/bin/env python
#-*-coding:utf8-*-

"""
генерация с метаклассами для легкого чтения структуры и избавления от необходимости каждый раз читать файл конфигурации
"""

class Table:
    pass

class View:
    pass

class Field:
    pass

class FieldCalc:
    pass

class FieldConst:
    pass

class MetaObjectType(type):
    """
    формирование класса метаданных на основе описания полей
    """

    def __new__(cls, *args, **kwargs):
        type.__new__(cls, args, kwargs)
