import requests
import functools
import datetime as dt
import pandas as pd
import psycopg2
from typing import Callable
import cytoolz
import json, sys
sys.path.append('/home/david/Dropbox/program/mypackage_py')
import sqlCommand as sqlCommand
import sqlite3
import logging
import rx

# 產生新的logger
logger = logging.Logger('test')

# 定義 handler 輸出 sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# 設定輸出格式
formatter = logging.Formatter('%(asctime)s: %(levelname)-s %(module)s %(lineno)d  %(funcName)s %(message)s')
# handler 設定輸出格式
console.setFormatter(formatter)

logger.addHandler(console)

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class NoData(Error):
    def __init__(self, expression = None):
        self.expression = expression


def requests_get(url: str, playload={}) -> str:
    response = requests.get(url, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    plain_text = response.text
    return plain_text


def requests_post(url: str, playload={}) -> str:
    response = requests.post(url, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    plain_text = response.text
    return plain_text


def session_get(session, url: str, playload={}) -> str:
    response = session.get(url, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    plain_text = response.text
    return plain_text


def session_post(session, url: str, playload={}) -> str:
    response = session.post(url, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    plain_text = response.text
    return plain_text


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'headers': headers}

plainTextF_get = cytoolz.curry(requests_get, playload=payload)
plainTextF_post = cytoolz.curry(requests_post, playload=payload)
# plainTextF_get = functools.partial(requestsF_get, playload=payload)
# plainTextF_post = functools.partial(requestsF_post, playload=payload)


def get_plain_text(url):
    return plainTextF_get(url)


@cytoolz.curry
def input_date(lastdate: dt.datetime, t: int) -> str:
    dateTime = lastdate + dt.timedelta(days=t)
    month, day = dateTime.month, dateTime.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(dateTime.year) + str(month) + str(day)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    return input_date


@cytoolz.curry
def input_dates(lastdate: dt.datetime, now: dt.datetime) -> list:
    delta = now - lastdate
    date = input_date(lastdate)
    return list(map(date, range(delta.days + 1)))


def last_datetime(conn, table: str) -> dt.datetime:
    df_distinct_date = sqlCommand.selectDistinct(['年月日'], table, conn)
    list_last_date = [int(i) for i in df_distinct_date.sort_values(['年月日']).iloc[-1][0].split('-')]
    lastdate = dt.datetime(list_last_date[0], list_last_date[1], list_last_date[2])
    return lastdate


def time_delta(lastdate: dt.datetime) -> dt.timedelta:
    return dt.datetime.now() - lastdate


def craw_save(crawler: Callable, saver: Callable[[pd.DataFrame], None], t) -> None:
    df = crawler(t)
    saver(df)


# def looper(crawAndSave: Callable, dates: list) -> Callable:
#     for date in dates:
#         try:
#             yield date, crawAndSave(date)
#         except NoData as e:
#             print(e)
#             pass


# def looper(crawAndSave: Callable, dates: list) -> Callable:
#     for date in dates:
#         try:
#             yield date, crawAndSave(date)
#         except Exception as e:
#             logger.warning('t:%s, e:%s', date, e)
#             pass

@cytoolz.curry
def handle_err(crawAndSave: Callable, date: str) -> None:
    try:
        crawAndSave(date)
    except NoData as e:
        print(date, e)


class CrawlerObserver(rx.Observer):

    def on_next(self, value):
        pass

    def on_completed(self):
        print("Done!")

    def on_error(self, error):
        logger.error(error)
        raise type(error)(error)


def loop(crawAndSave: Callable, dates: list):
    f = handle_err(crawAndSave)
    source = rx.Observable.from_(dates).map(f)
    source.subscribe(CrawlerObserver())

