from unittest import TestCase
import pytds
import os
from v7client import config

cfg = config.Config.build_from_env()

def connection():
    mcfg = cfg.MSSQL_CONFIG
    return pytds.connect(mcfg.SQL_HOST, mcfg.SQL_DB, mcfg.SQL_USER, mcfg.SQL_PWD)


class TestMsSql(TestCase):
    def test_query(self):
        with connection() as cnx:
            with cnx.cursor() as cursor:
                cursor.execute("SELECT 1 as sss")
                print('descr:', cursor.description)
                data = cursor.fetchone()
                print(data)
                assert data == (1,)
