import os, sys
from v7client import md_reader

def get_data_file(filename):
    return os.path.join(os.path.dirname(__file__), 'data', filename)

file_md = get_data_file('1Cv7.MD')
file_dds = get_data_file('1Cv7.DDS')
file_ert = get_data_file('test.ert')

def read_md():
    r = md_reader.parse_md(file_md)
    m = md_reader.extract_metadata(r)
    m.dbo_name = 'work'
    return m
