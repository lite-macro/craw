import requests
import functools
import datetime
import sqlite3
import psycopg2
import datetime, json, sys
sys.path.append('/home/david/Dropbox/program/mypackage')
import sqlCommand as sqlCommand
connLite = sqlite3.connect('C:\\Users\\user\\Documents\\db\\tse.sqlite3')

def requests_get(url, playload):
    source_code = requests.get(url, params=playload)
    source_code.encoding = 'utf-8'
    plain_text = source_code.text
    return plain_text

def requests_post(url, playload):
    source_code = requests.post(url, params=playload)
    source_code.encoding = 'utf-8'
    plain_text = source_code.text
    return plain_text

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'headers': headers}

plainText_get = functools.partial(requests_get, playload=payload)
plainText_post = functools.partial(requests_post, playload=payload)

def inputDate(lastdate, t):
    dateTime = lastdate + datetime.timedelta(days=t)
    month, day = dateTime.month, dateTime.day
    if len(str(month)) == 1:
        month = '0' + str(month)
    if len(str(day)) == 1:
        day = '0' + str(day)
    input_date = str(dateTime.year) + str(month) + str(day)
    print(dateTime.year, dateTime.month, dateTime.day, input_date)
    return input_date

def lastDate(tablename):
    df_distinct_date = sqlCommand.selectDistinct(['年月日'], tablename, connLite)
    list_last_date = [int(i) for i in df_distinct_date.sort_values(['年月日']).iloc[-1][0].split('-')]
    lastdate = datetime.datetime(list_last_date[0], list_last_date[1], list_last_date[2])
    return lastdate

def timeDelta(lastdate):
    return datetime.datetime.now() - lastdate

def crawToSqlite(f, tablename, *arg):
    global t, df
    lastdate = lastDate(tablename)
    timedelta = timeDelta(lastdate)
    for t in range(timedelta.days+1):
        try:
            df = f(lastdate, t, *arg)
            yield t, sqlCommand.insertData(tablename, df, connLite)
        except Exception as e:
            print(t, e)
            pass
