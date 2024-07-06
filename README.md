# Библиотека доступа к базам 1C7.7

Читает 1Cv7.md файл и на основании его структуры парсит sql запросы, заменяя названия метаданных на sql подходящий для выполнения

## Установка

`pipenv install "git+https://github.com/WorldException/v7client.git@0.3.2#egg=v7client"`

## Использование

`v7client` Парсер запросов.

```python
from v7client import config, base
work_config = Config(
    name='work',
    path_type='smb',
    smb_server=DefaultConfig.SMB_SERVER,
    smb_share=DefaultConfig.SMB_SHARE,
    smb_user=DefaultConfig.SMB_USER,
    smb_pwd=DefaultConfig.SMB_PWD,
    # sql_host=DefaultConfig.SQL_1C_HOST,
    # sql_db=DefaultConfig.SQL_1C_DB,
    # sql_user=DefaultConfig.SQL_1C_USER,
    # sql_pwd=DefaultConfig.SQL_1C_PWD,
    # update_interval=timedelta(days=1),
    path_to_base="/work"
)

db = Base(work_config, caching=True, use_dba=True)

# выборка текущих статусов для обновления в емекс
query_statuses = u"""
select
    d.НаСайт НаСайт,
    ж.НомерДок НомерДок,
    ж.ДатаДок ДатаДок,
    case t.#Статус $Перечисление.СтатусыЗаказа.case Статус,
    h.Наименование Характеристика,
    t.#КоличествоВБазе ВБазе,
    t.#КоличествоНаСайте НаСайте,
    t.#ОжидаемаяДата Ожидаем
from $Документ.Уведомление d (nolock)
    join $Документ.#Уведомление t (nolock) on d.Код=t.iddoc
    join $Справочник.ХарактеристикиНоменклатуры h (nolock) on t.#ХарактеристикаТовара = h.id
    join $Журнал ж (nolock) on ж.IDDOC = d.Код
where ж.ДатаДок between %(start)s and %(end)s
    and ж.ВидДокумента = d.ВидДокумента
    and h.Наименование like 'L%%'
"""

q = db.query(queryr_statuses)
q.set_param('start', start_date)
q.set_param('end_', end_date)
print(q)
for item in q():
    print(item)
```
---
`v7types` Работа с сериализацией во внутренние типы 1С СписокЗначений и ТаблицаЗначений.

```python
import v7types

table_value = v7types.loads(ТЗ_из_строки)
```

## Настройка

Через переменные окружения, либо создание с параметрами config.Config

Пример переменных окружения
```ini
V7_SQL_HOST=192.168.xx.xx
V7_SQL_DB=db
V7_SQL_USER=user
V7_SQL_PWD=pwd

V7_PATH_TO_BASE=/path_to_1cmd

V7_SMB_SERVER=192.168.xx.xx
V7_SMB_SHARE=remote_path
V7_SMB_USER=remote_user
V7_SMB_PWD=password
```

## Команды

python -m v7client --help

```bash
Usage: python -m v7client [OPTIONS] COMMAND [ARGS]...

Options:
  -p, --path TEXT  Путь к файлам 1Cv7.MD, 1Cv7.DBA
  --help           Show this message and exit.

Commands:
  download  Скачать метаданные во временное хранилище
  info      Получить информацию о базе
  parse     Парсить запрос в готовый sql
  ping      проверить соединение с базой данных
  query     Выполнить запрос и вернуть данные в виде csv
```
