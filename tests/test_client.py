from unittest import TestCase
from v7client import config, base
import os

test_config = config.Config(
    name='data',
    path_type='dir',
    path_to_base='./tests/data',
    sql_db=os.getenv('V7_SQL_DB'),
    sql_host=os.getenv('V7_SQL_HOST'),
    sql_pwd=os.getenv('V7_SQL_PWD'),
    sql_user=os.getenv('V7_SQL_USER'),
)



class TestClient(TestCase):

    def test_open(self):
        client = base.Base(config=test_config)
        print(client.x("Справочник.Номенклатура"))

    def test_query(self):
        client = base.Base(test_config, False, False, {'timeout': 60})
        q = client.query("select top 10 n.Наименование, n.Код  from $Справочник.Номенклатура as n")
        for i in q():
            print(i)

        print(list(q.as_list(dict)))

        print(list(q.as_dict_list()))
