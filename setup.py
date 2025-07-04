#!/usr/bin/env python
#-*-coding:utf8-*-

import os
from setuptools import setup

def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read()
    except IOError:
        return ''

requires = [
    'python-tds',
    'olefile',
    'click',
    'transliterate',
    "pysmbclient"
]

setup(
    name="v7client",
    version="0.3.7",
    license="GPL",
    description='1Cv7 client',
    long_description=read("README.md"),
    author="Stoyanov Evgeny",
    author_email="quick.es@gmail.com",
    url="https://github.com/WorldException/v7client",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: Russian',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.11',
        'Topic :: Software Development',
    ],
    packages=['v7client', 'v7types'],
    keywords='1C',
    #requires=requires,
    install_requires=requires,
)
