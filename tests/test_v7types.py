#-*-coding:utf8-*-
from __future__ import unicode_literals
import unittest
import v7types
import logging
import json
import re

mylog = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

logging.getLogger('v7types').setLevel(logging.DEBUG)


class TestV7Types(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_Table_to_Dict(self):
        tb = [
            ['col1', 'col2'],
            [1, 2],
            ['t1', 't2']
        ]
        td = v7types.dumps(tb)
        print(td)

    def test_loads(self):
        text1 = u'{"ТаблицаЗначений","2",{"0","","0","0","0","","2",{{"Кол1","Кол1","1","0","0",{"Строка",""},"","0",{{"Строка","тест ""что то в кавычкаъх"" фф"}}},{"Кол2","Кол2","1","0","1",{"Строка",""},"","0",{{"Строка","1231"}}}}}}'
        d = v7types.loads(text1)
        print(str(d))
        print(str(v7types.loads(text1)))
        print('revert')
        print(text1)
        print(str(v7types.dumps(d)))

    def test_list(self):
        test1 = '{"СписокЗначений",{{{"Строка","1234"},"Парам1","0"},{{"Число","222.2"},"Парам2","0"}}}'
        t = v7types.V7List.loads(test1)
        print(t)
        print("dumps")
        print(test1)
        print(v7types.V7List.dumps(t))

    def test_inherited(self):
        data = '{"СписокЗначений",{{{"ТаблицаЗначений","2",{"0","","0","0","0","","2",{{"КСтрока","КСтрока","1","0","0",{"Строка",""},"","0",{{"Строка","Чтототам"}}},{"КЧисло","КЧисло","1","0","1",{"Число","0"},"","2",{{"Число","11223344"}}},{"КСписок","КСписок","1","0","2",{},"","0",{{"СписокЗначений",{{{"Строка","223344"},"сзЗначение1","0"},{{"Число","114433"},"сзЗНачение2","0"}}}}}}}},"значТЗ","0"}}}'
        data = '{"СписокЗначений",{{{"ТаблицаЗначений","2",{"0","","0","0","0","","2",{{"КСтрока","КСтрока","1","0","0",{"Строка",""},"","0",{{"Строка","Чтототам"},{"Строка","Чтототам222"},{"Строка","Чтототам33"}}},{"КЧисло","КЧисло","1","0","1",{"Число","0"},"","2",{{"Число","1122.3344"},{"Число","444.444"},{"Число","44555"}}},{"КСписок","КСписок","1","0","2",{},"","0",{{"СписокЗначений",{{{"Строка","223344"},"сзЗначение1","0"},{{"Число","114433"},"сзЗНачение2","0"}}},{"Строка","aaaa"},{"Строка","aaaa4"}}},{"ТестКолонка4","ТестКолонка4","1","0","3",{},"","0",{{},{},{}}}}}},"значТЗ","0"}}}'
        v = v7types.loads(data)
        print("value")
        print(v)
        print("as json")
        print(json.dumps(v, indent=2))
        print("revert")
        print(data)
        data2 = v7types.dumps(v)
        print(data2)
        print(v7types.loads(data2))

    def test_dumps11(self):
        t = v7types.dumps({
            'test': [
                ['k1', 'k2'],
                ['123', '222']
            ]
        })
        print(t)

    def test_double(self):
        t = '["ТаблицаЗначений","2",["0","","0","0","0","","2",[["Кол1","Кол1","1","0","0",["Строка",""],"","0",[["Строка","тест ""что то в кавычкаъх"" фф"]]],["Кол2","Кол2","1","0","1",["Строка",""],"","0",[["Строка","1231"]]]]]]'
        for a in re.findall('(."".)', t):
            print(a)
        from v7types import v7types
        a = v7types.v7str_as_json(t)
        print(a)

'''
ТаблицаЗначений
{"ТаблицаЗначений","2",{"0","","0","0","0","","2",{{"Кол1","Кол1","1","0","0",{"Строка",""},"","0",{{"Строка","тест"}}},{"Кол2","Кол2","1","0","1",{"Строка",""},"","0",{{"Строка","1231"}}}}}}

ToDo сделать разбор вложенных объектов и преобразование в json
Процедура ЗначВСтроку()
    сз = СоздатьОбъект("СписокЗначений");
    сз.Установить("сзЗначение1", "223344");
    сз.Установить("сзЗНачение2", 114433);
    тз = СоздатьОбъект("ТаблицаЗначений");
    тз.НоваяКолонка("КСтрока","Строка");
    тз.НоваяКолонка("КЧисло","Число");
    тз.НоваяКолонка("КСписок");
    тз.НоваяСтрока();
    тз.КСтрока = "Чтототам";
    тз.КЧисло = 11223344;
    тз.КСписок = сз;
    
    сз2 = СоздатьОбъект("СписокЗначений");
    сз2.Установить("значТЗ", тз);
    т = СоздатьОбъект("Текст");
    т.ДобавитьСтроку(ЗначениеВСтроку(сз2));
    т.Показать();
КонецПроцедуры

{"СписокЗначений",{{{"ТаблицаЗначений","2",{"0","","0","0","0","","2",{{"КСтрока","КСтрока","1","0","0",{"Строка",""},"","0",{{"Строка","Чтототам"}}},{"КЧисло","КЧисло","1","0","1",{"Число","0"},"","2",{{"Число","11223344"}}},{"КСписок","КСписок","1","0","2",{},"","0",{{"СписокЗначений",{{{"Строка","223344"},"сзЗначение1","0"},{{"Число","114433"},"сзЗНачение2","0"}}}}}}}},"значТЗ","0"}}}
'''
