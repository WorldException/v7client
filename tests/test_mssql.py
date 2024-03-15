from unittest import TestCase
import pytds
import os


def connection():
    return pytds.connect(os.environ['MS_HOST'], os.environ['MS_DB'], os.environ['MS_USER'], os.environ['MS_PWD'])


class TestMsSql(TestCase):
    def test_query(self):
        with connection() as cnx:
            with cnx.cursor() as cursor:
                cursor.execute("SELECT 1 as sss")
                print('descr:', cursor.description)
                data = cursor.fetchone()
                print(data)
                assert data == (1,)
