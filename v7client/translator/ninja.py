#!/usr/bin/env python
#-*-coding:utf8-*-

from jinja2 import Environment, PackageLoader
import os

env = Environment(loader=PackageLoader('v7', os.path.join('translator', 'templates')),)

def exception_handler(exception):
    raise exception

env.exception_handler = exception_handler
