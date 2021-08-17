import mysql.connector as db
from mysql.connector.cursor import MySQLCursor
from connectors.requirement_keys import *
import pandas as pd

class system_database:
    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.create_connection()
        self.cursor = MySQLCursor(self.con)

    def create_connection(self):
        self.con = db.connect(host=self.host, user=self.user,
                     password=self.password,
                     database=self.db)
        # self.cursor = MySQLCursor(connection=self.con)
        self.cursor = self.con.cursor(buffered=True)
        self.con.commit()

    def _cursor(self):
        MySQLCursor(self.con)
        self.con.commit()

    def close_db_connection(self):
        self.con.close()

    def create_table(self, symbol, interval):
        # self.cursor.execute(operation=sql)
        # self.cursor.execute(operation=f'CREATE TABLE IF NOT EXISTS {symbol}_{interval}(High_price FLOAT(8,8), Open_price FLOAT(8,8), Close_price FLOAT(8,8), Low_price FLOAT(8,8), Volume VARCHAR(255))')
        self.cursor.execute(operation=f'CREATE TABLE IF NOT EXISTS {symbol}_{interval}(High_price FLOAT(8,2), Open_price FLOAT(8,2), Close_price FLOAT(8,2), Low_price FLOAT(8,2))')
        # self.cursor.execute(operation=f'CREATE TABLE IF NOT EXISTS {symbol}_{interval}(High_price BIGINT, Open_price BIGINT, Close_price BIGINT, Low_price BIGINT)')
        self.con.commit()

    def create_table_close_candlestick(self, symbol, interval):
       self.cursor.execute(
         operation=f'CREATE TABLE IF NOT EXISTS {symbol}_{interval}_closed(High_price FLOAT(8,2), Open_price FLOAT(8,2), Close_price FLOAT(8,2), Low_price FLOAT(8,2))')
       self.con.commit()

    def ML_create_table(self, operation):
        self.cursor.execute(operation=operation)
        self.con.commit()

    def commit(self):
        self.con.commit()

    def save_to_db(self, operation, price):
        self.cursor.execute(operation=operation, params=price)
        self.con.commit()

    def any_required_operation(self, operation):
        self.cursor.execute(operation=operation)
        self.con.commit()

    def required_operation_2_para(self, operation, data):
        self.cursor.execute(operation=operation, params=data)

    def return_all_from_db(self, operation):
        self.cursor.execute(operation=operation)
        return self.cursor.fetchall()

    def return_prices_df(self,operation, columns):
        self.cursor.execute(operation=operation)
        # df = pd.read_sql(sql=operation, con=self.create_connection, columns=columns)
        df = self.cursor.fetchall()
        return pd.DataFrame(data=df, columns=columns)

    def return_prices_csv(self, operation, columns):
        self.cursor.execute(operation=operation)
        # df = pd.read_sql(sql=operation, con=self.create_connection, columns=columns)
        df = self.cursor.fetchall()
        d = pd.DataFrame(data=df, columns=columns)
        d.to_csv('data_set_ml', index=True)
        return pd.read_csv('data_set_ml')





