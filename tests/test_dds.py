from unittest import TestCase
from v7client import dba
from .data import get_data_file
import logging

mylog = logging.getLogger(__name__)


class TestDDS(TestCase):

    def test_read(self):
        server, db, user, pwd = dba.read_dba(get_data_file('1Cv7.DBA'))
        mylog.debug("Server: %s" % server)