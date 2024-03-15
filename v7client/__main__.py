#!/usr/bin/env python
#-*-coding:utf8-*-
import click
import os
from .config import Config
from .base import Base


class CliConfig(Config):
    NAME = 'cli'
    # PATH_TO_BASE = os.path.abspath(path)
    PATH_TYPE = 'dir'  # smb|dir|ftp


@click.group()
@click.option('--path', '-p', default='.', help=u"Путь к файлам 1Cv7.MD, 1Cv7.DBA")
def v7(path):
    CliConfig.PATH_TO_BASE = os.path.abspath(path)


def get_db():
    db = Base(CliConfig, caching=False)
    return db


@click.command('download')
def v7_download():
    u"""
    Скачать метаданные во временное хранилище
    """
    db = get_db()
    db.download()
    click.echo('download complete')


@click.command('parse')
@click.option('--verbose', '-v', is_flag=True, help=u"Выводить подробно")
@click.argument('text')
def v7_parse(text, verbose):
    u"""
    Парсить запрос в готовый sql
    """
    q = get_db().query(text)
    if verbose:
        click.echo(str(q))
    else:
        click.echo(q.v7)


@click.command('query')
@click.option('--delimeter', '-d', default=u';', help=u"Разделитель")
@click.option('--strip', '-s', is_flag=True, help=u"Урезать пустые строки")
@click.argument('text')
def v7_query(text, delimeter, strip):
    u"""
    Выполнить запрос и вернуть данные в виде csv
    """
    delimeter = str(delimeter)
    q = get_db().query(text)
    for item in q():
        if strip:
            click.echo(delimeter.join([str(val).strip() for val in item]))
        else:
            click.echo(delimeter.join([str(val) for val in item]))


v7.add_command(v7_parse)
v7.add_command(v7_query)
v7.add_command(v7_download)

v7()
