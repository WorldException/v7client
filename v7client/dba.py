#!/usr/bin/env python
# -*-coding:utf8-*-
from typing import TypedDict
import logging
mylog = logging.getLogger(__name__)


class DbaDict(TypedDict):
    Server:str
    DB:str
    UID:str
    PWD:str
    Checksum:str


def read_dba(filename, dict_ret=False) -> DbaDict:
    '''
    Получает из файла dba параметры подключения к базе
    '''
    buff = ''
    with open(filename, 'rb') as f:
        buff = f.read()
    mylog.debug('buff [%s]' % buff)
    key = "19465912879oiuxc ensdfaiuo3i73798kjl".encode("US-ASCII")
    mylog.debug(key)
    ##decode[i] = ((byte)(buf[i] ^ SQLKeyCode[(i % 36)]));
    decode = []
    s = ''
    for i in range(0, len(buff)):
        b = buff[i] ^ key[(i % 36)]
        decode.append( b )
        s += chr(b)
    mylog.debug(s)
    s = s.replace('","','":"').replace('{{','{').replace('}}','}').replace('},{',',')
    #mylog.debug(s)
    v = eval(s)
    #mylog.debug(repr(v))
    if dict_ret:
        return v
    Server, DB, Login, Password = v["Server"], v["DB"], v["UID"], v["PWD"]
    mylog.debug(repr([Server, DB, Login, Password]))
    return Server, DB, Login, Password
    #{{"Server","192.168.1.30"},{"DB","work"},{"UID","sa"},{"PWD","xxxeral!"},{"Checksum","bf062f4f"}}

