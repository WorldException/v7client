#!/usr/bin/env python
# -*-coding:utf8-*-

import sqlalchemy

"""
https://docs.sqlalchemy.org/en/13/orm/mapped_attributes.html#mapper-hybrids
https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html#sqlalchemy.ext.declarative.DeferredReflection

class MyClass(Base):
    __tablename__ = 'my_table'

    id = Column(Integer, primary_key=True)
    job_status = Column(String(50))

    status = synonym("job_status")
    
>>> print(MyClass.job_status == 'some_status')
my_table.job_status = :job_status_1

>>> print(MyClass.status == 'some_status')
my_table.job_status = :job_status_1
"""