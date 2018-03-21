import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
import sqlite3, sqlCommand, psycopg2
import craw.crawler as crawler
from functools import partial
import pandas as pd

connLite = sqlite3.connect('/home/david/Documents/db/tse.sqlite3')
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")

from pymongo import MongoClient
client = MongoClient()
client = MongoClient('localhost', 27017)
mongoDb = client['tse']


def saveToSqliteF(table: str, df: pd.DataFrame) -> None:
    sqlCommand.i_lite(connLite, table, df)
    # sqlCommand.insertData(table, df, connLite)


def saveToPostgreF(table: str, df: pd.DataFrame) -> None:
    sqlCommand.insertDataPostgre(table, df, conn)


def saveToMongoF(table: str, df: pd.DataFrame) -> None:
    d = df.to_dict(orient='records')
    collection = mongoDb[table]
    collection.insert_many(d)


def saveToSqliteMongoF(table: str, df: pd.DataFrame) -> None:
    saveToSqliteF(table, df)
    saveToMongoF(table, df)


last_datetime = partial(crawler.last_datetime, connLite)

# df = sqlCommand.selectAll('個股日本益比、殖利率及股價淨值比', connLite)
# saveToMongoF('個股日本益比、殖利率及股價淨值比', df)
