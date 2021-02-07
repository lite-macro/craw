import time
import rx
import logging
import sqlCommand as sqlc
import requests
import datetime as dt
import pandas as pd
from typing import Callable, Iterable, List
import cytoolz
import os


# 產生新的logger
logger = logging.Logger('test')

# 定義 handler 輸出 sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# 設定輸出格式
formatter = logging.Formatter(
    '%(asctime)s: %(levelname)-s %(module)s %(lineno)d  %(funcName)s %(message)s')
# handler 設定輸出格式
console.setFormatter(formatter)

logger.addHandler(console)


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class NoData(Error):
    def __init__(self, expression=None):
        self.expression = expression


@cytoolz.curry
def requests_get(url: str, playload: dict) -> requests.Response:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
    response = requests.get(url, headers=headers, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response


@cytoolz.curry
def requests_post(url: str, playload: dict) -> requests.Response:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
    response = requests.post(url, headers=headers, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response


@cytoolz.curry
def session_get(session, url: str, playload: dict) -> requests.Response:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
    response = session.get(url, headers=headers, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response


@cytoolz.curry
def session_post(session, url: str, playload: dict) -> requests.Response:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
    response = session.post(url, headers=headers, params=playload)
    print(response.url)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response


@cytoolz.curry
def session_get_text(session, url: str, playload: dict) -> str:
    return session_get(session, url, playload).text


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


def __dt_to_str(date: dt.datetime) -> str:
    month, day = date.month, date.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(date.year) + str(month) + str(day)
    return input_date


def dt_to_str(dates: Iterable[dt.datetime]) -> List[str]:
    days = sorted([__dt_to_str(date) for date in dates])
    print(days)
    return days


@cytoolz.curry
def input_dates(lastdate: dt.datetime, now: dt.datetime) -> list:
    delta = now - lastdate
    date = input_date(lastdate)
    return list(map(date, range(delta.days + 1)))


@cytoolz.curry
def last_datetime(conn, table: str) -> dt.datetime:
    df_distinct_date = sqlc.selectDistinct(['年月日'], table, conn)
    list_last_date = [int(i) for i in df_distinct_date.sort_values(
        ['年月日']).iloc[-1][0].split('-')]
    lastdate = dt.datetime(
        list_last_date[0], list_last_date[1], list_last_date[2])
    return lastdate


def time_delta(lastdate: dt.datetime) -> dt.timedelta:
    return dt.datetime.now() - lastdate


@cytoolz.curry
def craw_save(saver: Callable[[pd.DataFrame], None], crawler: Callable, t) -> None:
    saver(crawler(t))


@cytoolz.curry
def looper(crawAndSave: Callable, dates: list) -> Callable:
    for date in dates:
        time.sleep(5)
        try:
            yield date, crawAndSave(date)
        except NoData as e:
            print(date, e)


@cytoolz.curry
def handle_err(crawAndSave: Callable, date: str) -> None:
    try:
        crawAndSave(date)
    except NoData as e:
        print(date, e)


class CrawlerObserver():

    def on_next(self, value):
        pass

    def on_completed(self):
        print("Done!")

    def on_error(self, error):
        logger.error(error)
        raise type(error)(error)


# def loop(crawAndSave: Callable, dates: list):
#    f = handle_err(crawAndSave)
#    source = rx.Observable.from_(dates).map(f)
#    source.subscribe(CrawlerObserver())
