#!/usr/bin/env python
#-*-coding:utf8-*-


from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


import logging
dblog = logging.getLogger(__name__+'.database')
mdlog = logging.getLogger(__name__+'.Models')

base = declarative_base()


class BaseModel:
    pref = '%s'

    def __init__(self, db):
        self.db = db
        pass

    def create_tables(self, db):
        pass

    def drop_tables(self, db):
        yield

    def prf(self, value):
        return self.pref % value


class DB:
    """
    доступ через sqlalchemy к одной базе данных,
    инициализирует подключение, сессию и модели
    свойство models представляет собой ссылку на класс с моделями
    """
    #models = None
    base_ = base

    def __init__(self, dburl, echo=False):
        #engine = create_engine(cnx_string, echo=False, connect_args={'check_same_thread':False}) #, convert_unicode=True
        dblog.info('create engine: %s' % dburl)

        self.engine = create_engine(dburl, echo=echo, logging_name=self.__class__.__name__, pool_pre_ping=True) #, convert_unicode=True
        self.session = scoped_session(sessionmaker(autocommit=True, autoflush=True, bind=self.engine))
        self.md = MetaData(bind=self.engine)
        #self.session = sessionmaker(autocommit=True, autoflush=True, bind=self.engine)()
        self.base = self.base_  # declarative_base(bind=self.engine, name=self.__class__.__name__, metadata=self.md)
        self.base.metadata.bind = self.engine
        self.base.query = self.session.query_property()
        self.base_type = type(self.base)
        self.query = self.session.query
        self.bind_key = self.__class__.__name__

    def print_info(self):
        dblog.info('DB:'+self.__class__.__name__)
        dblog.info('Engine: %s' % self.engine)
        dblog.info('Session: %s' % self.session)
        dblog.info('Base: %s' % self.base)
        for name, table in self.base.metadata.tables.items():
            dblog.info('Table: %s' % name)

    def load_models(self):
        """
        этот метод надо перекрыть в потомках для подгрузки моделей
        """
        self.models = BaseModel()

    @classmethod
    def from_config(cls, config):
        dblog.setLevel(config.LOG_LEVEL)
        cnx_string = config.SQLALCHEMY_URL()
        #engine = create_engine(cnx_string, echo=False, connect_args={'check_same_thread':False}) #, convert_unicode=True
        dblog.info('config:'+config.__name__)

        _db_ = cls(cnx_string, config.SQLALCHEMY_ECHO)
        _db_.load_models()
        return _db_

    def drop_tables(self):
        for model in self.models.drop_tables(self):
            if model:
                try:
                    dblog.info('DROP TABLE: %s, %s' % (model.__name__, model.__tablename__))
                    table = self.base.metadata.tables[model.__tablename__]
                    table.drop(bind=self.engine)
                except Exception as ed:
                    dblog.error(u'error drop %s, %s' % (model.__tablename__, ed.message))

    def create_tables(self, recreate=False):
        if recreate:
            self.drop_tables()

        for model in self.models.create_tables(self):
            if model:
                try:
                    dblog.info('CREATE TABLE: %s, %s' % (model.__name__, model.__tablename__))
                    table = self.base.metadata.tables[model.__tablename__]
                    table.create(bind=self.engine)
                except Exception as ec:
                    dblog.exception(ec.message)

