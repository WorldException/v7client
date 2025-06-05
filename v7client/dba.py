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


def read_dba_tuple(filename) -> tuple[str, str, str, str]:
    '''
    Получает из файла dba параметры подключения к базе
    '''
    v = read_dba_dict(filename)
    Server, DB, Login, Password = v["Server"], v["DB"], v["UID"], v["PWD"]
    mylog.debug(repr([Server, DB, Login, Password]))
    return Server, DB, Login, Password
    

def read_dba(filename, dict_ret=False) -> DbaDict | tuple[str, str, str, str]:
    '''
    Получает из файла dba параметры подключения к базе
    '''
    if dict_ret:
        return read_dba_dict(filename)
    else:
        return read_dba_tuple(filename)


def read_dba_dict(filename) -> DbaDict:
    buff:bytes
    with open(filename, 'rb') as f:
        buff = f.read()
    #mylog.debug('buff [%s]' % buff.decode('cp1251'))
    key = "19465912879oiuxc ensdfaiuo3i73798kjl".encode("US-ASCII")
    #mylog.debug(key)
    ##decode[i] = ((byte)(buf[i] ^ SQLKeyCode[(i % 36)]));
    
    # Декодирование кодировки
    decode = []
    decoded_data = ''
    for i in range(0, len(buff)):
        b = buff[i] ^ key[(i % 36)]
        decode.append(b)
        decoded_data += chr(b)
    #mylog.debug(decoded_data)
    replaced_data = decoded_data.replace('","','":"').replace('{{','{').replace('}}','}').replace('},{',',')
    
    # преобразовать в структуру
    v = eval(replaced_data)
    mylog.debug(repr(v))

    return v