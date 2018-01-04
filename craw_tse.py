import sys
prefix = '/home/david/'
sys.path.append('{}Dropbox/program/mypackage_py'.format(prefix))
import sqlite3, sqlCommand, psycopg2
import craw.crawler_fp1 as crawler_fp1
from functools import partial

connLite = sqlite3.connect('{}Documents/db/tse.sqlite3'.format(prefix))
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")

from pymongo import MongoClient
client = MongoClient()
client = MongoClient('localhost', 27017)
mongoDb = client['tse']


def saveToSqliteF(tablename, df):
    sqlCommand.insertData(tablename, df, connLite)


def saveToPostgreF(tablename, df):
    sqlCommand.insertDataPostgre(tablename, df, conn)


def saveToMongoF(tablename, df):
    d = df.to_dict(orient='records')
    collection = mongoDb[tablename]
    collection.insert_many(d)


def saveToSqliteMongoF(tablename, df):
    saveToSqliteF(tablename, df)
    saveToMongoF(tablename, df)


last_datetime = partial(crawler_fp1.last_datetime, connLite)

# df = sqlCommand.selectAll('個股日本益比、殖利率及股價淨值比', connLite)
# saveToMongoF('個股日本益比、殖利率及股價淨值比', df)
