#-*-coding:utf8-*-
import logging, os
import tempfile


mylog = logging.getLogger('v7')

#logging.getLogger('v7.prepare').setLevel(logging.DEBUG)
#logging.getLogger('v7.metadata').setLevel(logging.DEBUG)


store = tempfile.gettempdir()
data_path = os.path.join(store, 'data')
os.makedirs(data_path, exist_ok=True)

def getDataPath(filename):
    return os.path.join(data_path, filename)


from .dds_parser import parse as parse_dds
from .md_reader import parse_md, extract_metadata
from .core import Application
from .db import MS_Proxy
from .metadata import FieldNotFound, MDErrorNotFound