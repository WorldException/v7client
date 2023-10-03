#!/usr/bin/env python
#-*-coding:utf8-*-
from __future__ import unicode_literals
import os
import logging
log = logging.getLogger(__name__)


class Maker(object):

    def __init__(self, config):
        self.config = config

    def make(self):
        #self.make_dba()
        #self.make_package()
        #self.make_connection()
        #self.make_classes()
        pass

    def make_src(self, filename):
        full_filename = os.path.join(self.config.PATH_TO_MODEL, filename)
        log.debug(full_filename)
        f = open(full_filename, 'w')
        return f

    def append_src(self, filename):
        f = open(os.path.join(self.config.PATH_TO_MODEL, filename), 'a')
        return f

