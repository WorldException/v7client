#!/usr/bin/env python
#coding:utf8

from __future__ import unicode_literals
from sqlalchemy import Unicode, Index, DateTime, Integer, Float, Numeric, BigInteger, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from PriceGears import config

epg = create_engine(config.PgSQLHetzner.SQLALCHEMY_URL(), echo=True)
Base = declarative_base(bind=epg)

'''
Надо сделать по этому пути.
https://docs.sqlalchemy.org/en/13/orm/extensions/automap.html
class Provider(Base):
    __tablename__ = 'producers'
    __table_args__ = (
        {'schema':'autoprice_avtosvet'}, 
        {'autoload':True}
    )
'''

Base.metadata.reflect(schema='autoprice_avtosvet')
m = Base.metadata
m.tables.keys()
t = Base.metadata.tables['autoprice_avtosvet.provider']
t.c.short_name


class Provider2(Base):
    __table__ = Base.metadata.tables['autoprice_avtosvet.provider']
    short_name2 = __table__.c.short_name

S = sessionmaker(bind=epg)
s = S()
for p in s.query(Provider2)[:10]:
    print(p.short_name2)

# ID https://docs.sqlalchemy.org/en/13/dialects/postgresql.html
