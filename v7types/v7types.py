#!python
#-*-coding:utf8-*-
from __future__ import unicode_literals
from typing import List, Optional, AnyStr, Iterator, Dict, Any, Union
import copy
import json
import logging
import re

mylog = logging.getLogger(__name__)


class V7Object(object):

    @classmethod
    def loads(cls, data: Union[str, list]) -> Union[list, dict]:
        ...

    @classmethod
    def dumps(cls, data: Union[list, dict]) -> str:
        ...


class V7Table(V7Object):
    """
    Преобразование ТаблицыЗначений

    """
    def __init__(self, data: List[Dict[str, Any]]):
        """

        :param data: Список словарей
        [
            dict()
            ...
        ]
        """
        # строки данных
        _rows = []
        self.columns_types = dict()
        if len(data) > 0:
            # каждая строка это dict
            _rows = data
            self.columns = list(data[0].keys())

            for col_name, col_value in data[0].items():
                col_type = type(col_value)
                if col_type in (int, float):
                    self.columns_types[col_name] = '{"Число","0"},"","2"'
                elif col_type is dict:
                    self.columns_types[col_name] = '{},"","0"'
                elif col_type is list:
                    self.columns_types[col_name] = '{},"","0"'
                else:
                    self.columns_types[col_name] = '{"Строка",""},"","0"'
        else:
            self.columns = []

        self.lineno = 0

        # значения колонок
        self.col_values = []
        # заголовки колонок
        self.col_head = []
        for n, col_name in enumerate(self.columns, 0):
            #self.col_head.append(f'{{"{cname}","{cname}","1","0","{n}",{{"Строка",""}},"","0",{{')
            # нетипизированная колонка, нужна для вложенных объектов
            # n - номер колонки, Тип, Длина, Точность
            col_type = self.columns_types.get(col_name, '{"Строка",""},"","0"')
            self.col_head.append(f'{{"{col_name}","{col_name}","1","0","{n}",{col_type},{{')
            self.col_values.append([])

        if _rows:
            self.extend(_rows)

    @property
    def nrows(self) -> int:
        return self.lineno

    def add(self, line: Dict):
        """
        line = {'col':val} or ['val1','val2']
        """
        self.append_row_dict(line)

    def extend(self, items: List[Dict]):
        for item in items:
            self.add(item)

    @property
    def ncols(self) -> int:
        return len(self.columns)

    def save(self, filename):
        pass

    def _make_header(self):
        return f'{{"ТаблицаЗначений","2",{{"0","","0","0","0","","2",{{'

    def add_column_value(self, col_num, value: int | float | list | dict | str):
        val_type = type(value)
        
        '''
        match str(type(value)):
            case "int" | "float":
                value = str(value)
                self.col_values[col_num].append(f'{{"Число","{value}"}}')
            case "list":

            case "dict":

            case "str":

            case _:
                pass
        '''

        if val_type in (int, float):
            value = str(value)
            self.col_values[col_num].append(f'{{"Число","{value}"}}')
        elif val_type is list:
            value = str(dumps(value))
            self.col_values[col_num].append(f'{value}')
        elif val_type is dict:
            value = str(dumps(value))
            self.col_values[col_num].append(f'{value}')
        else:
            # это обычная строка
            # замена на двойные кавычки
            value = str(value).replace('"', '""')
            self.col_values[col_num].append(f'{{"Строка","{value}"}}')

    def append_row_dict(self, item: Dict):
        for col_no, key in enumerate(self.columns, 0):
            value = item.get(key, '')
            self.add_column_value(col_no, value)

        self.lineno += 1

    def __str__(self):
        cols = []
        for i in range(0, self.ncols):
            cols.append(self.col_head[i] + ",".join((self.col_values[i])) + "}}")

        return self._make_header() + ",".join(cols) + "}}}"

    @classmethod
    def dumps(cls, obj: List) -> str:
        v = cls(obj)
        return str(v)

    @classmethod
    def loads(cls, data: Union[str, List[dict]]) -> List[dict]:
        """
        преобразует внутреннее представление таблицы 1С в массив, первой строкой идет
        наименование колонок
        """
        if type(data) is list:
            d = data
        elif type(data) is str:
            d = v7str_as_json(data)
        else:
            raise ValueError(f"Неподходящий тип {type(data)}")
        #колонки
        cols = []
        for line in d[2][7]:
            cols.append(line[0])
        ncols = len(cols)
        # Строки
        result = []
        nrows = len(d[2][7][0][8])  # количество строк первой колонки
        ncolsrange = range(0, ncols)
        rows = d[2][7]
        # имена колонок
        for row_num in range(0, nrows):
            row = dict()
            for col_num in ncolsrange:
                cell = rows[col_num][8][row_num]

                value = ''
                if cell:
                    try:
                        if len(cell) == 4:
                            # ['Справочник', 'ХарактеристикиНоменклатуры', '', 'СБ01200322']
                            # беру представление
                            cell_type = "Строка"
                            cell_value = cell[-1]
                        else:
                            cell_type, cell_value = cell
                        if cell_type == 'СписокЗначений':
                            value = V7List.loads(cell)
                        elif cell_type == 'ТаблицаЗначений':
                            value = V7Table.loads(cell)
                        elif cell_type == 'Число':
                            value = float(cell_value)
                        else:
                            value = cell_value
                    except Exception as e:
                        mylog.warning(str(cell))
                        raise

                row[cols[col_num]] = value
            result.append(row)
        return result


class V7TableFromList(V7Table):

    def __init__(self, data: List[List[Any]]):
        """

        :param data:
        Вариант со списоком
        [
            ["Колонка1", "Колонка2"],
            ... строки опционально
        ]
        """
        # строки данных
        _rows = []
        if len(data) > 0:
            _rows = data[1:]
            _columns = data[0]
            self.columns = _columns
        else:
            raise ValueError("Нельзя создавать с пустыми данными")

        _rows = list(list_as_dict(data))
        super(V7TableFromList, self).__init__(_rows)

    def add(self, line: List):
        """
        line = {'col':val} or ['val1','val2']
        """
        self.append_row_list(line)

    def append_row_list(self, item: List):
        if len(item) > len(self.columns):
            item = item[:len(self.columns)]
        if len(item) < len(self.columns):
            mylog.warning("Пропускаю строку, размер меньше заголовка")
            return None

        for col_no, value in enumerate(item, 0):
            val_type = type(value)
            self.add_column_value(col_no, value)

        self.lineno += 1

    @classmethod
    def loads(cls, data: Union[str, list]) -> List[List]:
        """
        преобразует внутреннее представление таблицы 1С в массив, первой строкой идет
        наименование колонок
        """
        if type(data) is list:
            d = data
        elif type(data) is str:
            d = v7str_as_json(data)
        else:
            raise ValueError(f"Неподходящий тип {type(data)}")
        #колонки
        cols = []
        for line in d[2][7]:
            cols.append(line[0])  # .encode('cp1251'))
        ncols = len(cols)
        # Строки
        data = [cols, ]
        nrows = len(d[2][7][0][8])  # количество строк первой колонки
        ncolsrange = range(0, ncols)
        rows = d[2][7]
        for r in range(0, nrows):
            row = []
            for c in ncolsrange:
                cell = rows[c][8][r]
                if cell:
                    cell_type, cell_value = cell
                    if cell_type == 'СписокЗначений':
                        row.append(V7List.loads(cell))
                    elif cell_type == 'ТаблицаЗначений':
                        row.append(V7Table.loads(cell))
                    else:
                        row.append(cell_value)  # .encode('cp1251'))
                else:
                    row.append('')
            data.append(row)
        return data


class V7List(V7Object):
    """
    {"СписокЗначений",{{{"Строка","1234"},"Парам1","0"},{{"Число","222.2"},"Парам2","0"}}}
    """
    def __init__(self, data: Dict):
        self.data = data

    def __str__(self) -> str:
        return self.dumps(self.data)

    @classmethod
    def loads(cls, data: Union[str, list]) -> Dict[str, Union[float, str, 'V7List', 'V7Table']]:
        """
        получить кортеж из внутреннего представления

        :param data:
        :return:
        """
        if type(data) is list:
            d = data
        elif type(data) is str:
            d = v7str_as_json(data)
        else:
            raise ValueError(f"Неподходящий тип {type(data)}")

        result = dict()
        for field in d[1]:
            type_value = field[0]
            if type_value[0] in ('ТаблицаЗначений', "СписокЗначений"):
                type_name, some, value = type_value
            else:
                type_name = type_value[0]
                value = type_value[1]
            name = field[1]

            if type_name == 'Число':
                result[name] = float(value)
            elif type_name == 'ТаблицаЗначений':
                result[name] = V7Table.loads(type_value)
            elif type_name == 'СписокЗначений':
                result[name] = V7List.loads(type_value)
            else:
                result[name] = value
        return result

    @classmethod
    def dumps(cls, data: dict) -> str:
        """

        {"СписокЗначений",
            {
                {
                    {"Строка","1234"},"Парам1","0"
                },
                {
                    {"Число","222.2"},"Парам2","0"
                }
            }
        }

        :param data:
        :return:
        """
        r = []
        for k, v in data.items():
            if type(v) in (int, float):
                type_name = "Число"
                type_value = v
                r.append(f'{{{{"{type_name}","{type_value}"}},"{k}","0"}}')
            elif type(v) is list:
                type_name = "ТаблицаЗначений"
                type_value = dumps(v)
                r.append(f'{{{type_value},"{k}","0"}}')
            elif type(v) is dict:
                type_name = "СписокЗначений"
                type_value = dumps(v)
                r.append(f'{{{type_value},"{k}","0"}}')
            else:
                type_name = "Строка"
                type_value = v
                r.append(f'{{{{"{type_name}","{type_value}"}},"{k}","0"}}')

        return '{"СписокЗначений",{'+",".join(r)+"}}"


def parse_value(type_name, value, type_value):
    if type_name == 'Число':
        return float(value)
    elif type_name == 'ТаблицаЗначений':
        return V7Table.loads(type_value)
    elif type_name == 'СписокЗначений':
        return V7List.loads(type_value)
    else:
        return value


def v7str_as_json(data: str) -> List:
    """
    попытка прочитать данные как json

    :param data:
    :return:
    """
    #{"ТаблицаЗначений","2",{"0","","0","0","0","","2",{{"Кол1","Кол1","1","0","0",{"Строка",""},"","0",{{"Строка","тест"}}},{"Кол2","Кол2","1","0","1",{"Строка",""},"","0",{{"Строка","1231"}}}}}}
    #v7data = v7str.replace('","', "','")\
    #    .replace('{"', "{'").replace('"}', "'}").replace('",{', "',{").replace('},"', "},'")
    v7data = data
    # замена '
    #v7data = data.replace("\\'", '\\"')
    #v7data = v7data.replace('","')
    # переносы строк становятся \n
    v7data = "\\n".join(v7data.splitlines())

    # v7data = v7data.replace("\n", "\\n")
    v7data = v7data.replace('{', '[')
    v7data = v7data.replace('}', ']')

    # попытка победить двойные кавычки нутри строк на \"
    without_double = v7data
    for double in re.findall('(."".)', v7data):
        if not (double[0] in (',', ']', '[') and double[-1] in (',', ']', '[')):
            mylog.debug(f"найдены двойные кавычки [{double}]")
            double_new = f'{double[0]}\\"{double[-1]}'
            without_double = without_double.replace(double, double_new)
    v7data = without_double

    try:
        d = json.loads(v7data)
    except:
        mylog.warning(v7data)
        raise
    return d


def loads(data: str) -> Union[Dict, List]:
    """
    Преобразовать представление 1С в dict, list

    :param data:
    :return: Union[Dict, List]
    """
    d = v7str_as_json(data)

    result = dict()

    type_name = d[0]
    if type_name == 'ТаблицаЗначений':
        result = V7Table.loads(d)
    elif type_name == 'СписокЗначений':
        result = V7List.loads(d)
    else:
        result = d
    return result


def dumps(obj):
    """
    Из python объекта в 1C

    :param obj:
    :return:
    """
    if type(obj) is list:
        if len(obj) > 0 and type(obj[0]) is dict:
            return V7Table.dumps(obj)
        else:
            return V7TableFromList.dumps(obj)
    elif type(obj) is dict:
        return V7List.dumps(obj)
    elif isinstance(obj, V7Table):
        return str(obj)
    elif isinstance(obj, V7List):
        return str(obj)
    return str(obj)


def repr_table(data, size=10):  # type: (List[List[AnyStr]], Optional[int]) -> Iterator[AnyStr]
    """
    представление таблицы для удобного логирования или печати

    :param data:
    :param size:
    :return:
    """
    fmt = u"{:>"+str(size)+"}"
    for line in data:
        yield u"|".join(map(lambda x: fmt.format(x), line))


def print_table(data, size=10):  # type: (List[List[AnyStr]], Optional[int]) -> None
    """
    Вывести в удобном виде данные
    :param data:
    :param size: размер колонки
    :return:
    """
    print("table rows:{}".format(len(data)))
    for line in repr_table(data, size):
        print(line)

def list_as_dict(data):  # type: (List[List[AnyStr]]) -> Iterator[Dict[AnyStr, Any]]
    """
    позволяет перебрать в качестве dict

    :param data:
    :return:
    """
    if len(data) > 1:
        head = data[0]
        for line in data[1:]:
            row = dict()
            for n, v in enumerate(line):
                row[head[n]] = v
            yield row
