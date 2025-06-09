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

    def test_download_concurency(self):
        import time, random
        from concurrent.futures import ThreadPoolExecutor
        
        def task(msg):
            cfg = config.Config.build_from_env()
            client = base.Base(config=cfg)
            print(f"start download {msg}")
            client.download()
            print(f"finish: {msg}")


        with ThreadPoolExecutor(4) as p:
            f = p.map(task, ["1", "2", "3", "4"])
        print(list(f))


