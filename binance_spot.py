from pushbullet import Pushbullet
import asyncio
import plotly.graph_objects as go
import yfinance as yf
from connectors.requirement_keys import *
import logging
import time
import pandas as pd
import requests
import hmac
import hashlib
from urllib.parse import urlencode
from stratigies.technical_signals import Signal
import ta
import time
import datetime
import websocket
import threading
import json
import typing
import pprint
from binance.client import Client
from models import *
from sqlalchemy import engine
import mysql.connector as db
from connectors.Sys_db import system_database
import talib
import pandas_ta as ta
import numpy as np
import datetime
from Interfaces.loggingFrame import loggingFrame
####
# PUSHBULLET notification
pb = Pushbullet(api_key=pushbullet2)
#CONSTANTS FOR TESTING
symbol = 'btcbnb'
timePeriod = '1m'

#DATABASE OF MYSQL
sys_db = system_database(host=mysql_host,user=mysql_user,password=mysql_pass,db='prices_crypto')
sys_db.commit()

logger = logging.getLogger()
# list into a tuple
# engines = engine.create_engine('sqlite:///dAtaBase.db')
#
# # engine = engine.create_engine('sqlite:///root@127.0.0.1:3306')
# con = db.connect(host=mysql_host,
#                         user=mysql_user,password=mysql_pass,database=mysql_database)
# cur = con.cursor()
# conn = system_database(host=mysql_host,
                        # user=mysql_user,password=mysql_pass, db=mysql_database)
#FOR STORING DATA (list)
closes, highs, lows, opens = [], [], [], []

class BinanceSpotClient:
    def __init__(self, apiKey: str, apiSecret: str, testnet: bool):
        self.root = loggingFrame
        if testnet:
            self._base_url = "https://testnet.binancefuture.com"
            self.wss_url =  "wss://stream.binancefuture.com/ws"
        else:
            self.base_url = "https://api.binance.com"
            self.wss_url = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'

        self._apiKey = apiKey
        self._apiSecret = apiSecret
        self.connection = Client(apiKey, apiSecret)
        self.open_orders = Client.get_open_orders
        self.prices = dict()
        self.df = pd.read_sql
        self.logs = []

    def _add_new_log(self, message: str):
        logger.info("%s", message)
        self.logs.append({"log": message, "displayed": False})

    async def get_balance(self):
        info = await self.connection.get_account()
        print("------------------- BALANCE -------------------")
        bal = info['balances']
        if bal:
            for b in bal:
                if float(b['free']) > 0:
                    print("SYMBOL: ", b['asset'], "\nFREE AMOUNT: ",b['free'],
                          "\nLOCKED AMOUNT: ", b['locked'])
                    print("============================================================")
                    balance_symbol = b['asset']
                    balance_free = b['free']
                    balance_locked = b['locked']
                    pb.push_note(title='BALANCE', body=f'SYMBOL: {balance_symbol}\nFREE AMOUNT: {balance_free}\nLOCKED: {balance_locked}')
        else:
            print("NO Balance")

    async def get_Open_orders(self):
        open_orders = await self.connection.get_open_orders()
        try:
            if open_orders:
                    print("-------------------- CURRENT OPEN ORDERS --------------------")
                    for orders in open_orders:
                        print("SYMBOL: ",orders['symbol'], "\nAT PRICE: "
                              ,orders['price'],"\nREQUESTED QUANTITY: ",orders['origQty'])
                        order_symbol = orders['symbol']
                        order_req = orders['origQty']
                        order_price = orders['price']
                        pb.push_note(title='ORDER',
                                     body=f'SYMBOL: {order_symbol}| QUANTITY: {order_req} | AT PRICE {order_price}')
            else:
                 print("No Open orders")
        except:
            logger.info("Timestamp for this request was ahead of the server's time\nSync O.S from settings")

    async def get_withdrawals(self):
        withdrawals = await self.connection.get_withdraw_history()
        print("------------------- WITHDRAWAL HISTROY -------------------")
        for exit in withdrawals['withdrawList']:
            print("ASSET: ", exit['asset'], "\nAMOUNT: ", exit['amount'], "\nNETWORK: ",
                  exit['network'], "\nTRANSACTION FEE: ", exit['transactionFee'])
            print("-----------------------------------------------------")
            asset = exit['asset']
            amount = exit['amount']
            network = exit['network']
            fee = exit['transactionFee']
            pb.push_note(title="WITHDRAWALS",
                         body=f'ASSET: {asset} | AMOUNT: {amount} | NETWORK: {network}, | FEE: {fee}')

    async def get_deposit(self):
        deposit = await self.connection.get_deposit_history()
        print("------------------- DEPOSIT HISTORY -------------------")
        if deposit['success']:
            for d in deposit['depositList']:
             print("ASSET: ", d['asset'], "\nAMOUNT: ", d['amount'])
             print("------------------------------------------------")
             asset = d['asset']
             amount = d['amount']
             pb.push_note(title='DEPOSIT',
                          body=f'ASSET: {asset} | AMOUNT: {amount}')

    async def get_order_history(self):
        orders = dict()
        re = self._make_request("GET","/api/v3/historicalTrades", orders)
        order_history = await self.connection.get_historical_trades()
        print("Orders History: ")
        for orders in order_history:
            print(orders)

    async def get_api_status(self):
        apiStat = await self.connection.get_account_api_trading_status()
        status = await self.connection.get_system_status()
        s_time = await self.connection.get_server_time()
        if status == 1:
            logger.info("SYSTEM MAINTENANCE BY BINANCE")
            pb.push_note(title='SYSTEM DOWN', body='SYSTEM MAINTENANCE BY BINANCE')
        else:
            logger.info("BINANCE SERVER IS UP")
            # timestamp = datetime.datetime.fromtimestamp(int(s_time['serverTime']))
            timestamp = int(s_time['serverTime'])
            print("SERVER TIME: ", timestamp)
            try:
                if apiStat['success']:
                    print("BINANCE API STATUS \n",
                          " -------- > TRUE = WORKING | FALSE = NOT CONNECTED < ---------")
                    print(apiStat['success'])
                    stat = apiStat['success']
                    pb.push_note(title='API STATUS',
                                 body=f'SERVER TIME: {timestamp} | STATUS: {stat}')
                else:
                    print("NO CONNECTION ESTABLISHED")
            except:
                logger.info("TIMESTAMP WAS AHEAD OF THE SERVERS TIME")
                msg = apiStat['msg']
                pprint.pprint(msg)

    def _on_message(self, ws: websocket, message):
        json_message = json.loads(message)
        candle = json_message['k']
        is_candle_close = candle['x']
        close = candle['c']
        high = candle['h']
        low = candle['l']
        vol = candle['v']
        open = candle['o']
        sybol = str(candle['s']).lower()
        interval_of_candle = str(candle['i'])
        num_of_trades = candle['n']
        time_of_event = [json_message['E']]
        sql_table = f'CREATE TABLE IF NOT EXISTS {sybol}_{interval_of_candle}(time BIGINT, High_price FLOAT(8,8), Open_price FLOAT(8,8), Close_price FLOAT(8,8), Low_price FLOAT(8,8), Volume DOUBLE(4,4))'
        sql_insert = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
        sql_insert_p = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price, Volume) VALUES (%s, %s, %s, %s, %s)'
        sql_get_all = f'SELECT * FROM {sybol}_{interval_of_candle}'
        sql_get = f'SELECT High_price, Open_price, Close_price, Low_price, Volume FROM {sybol}_{interval_of_candle}'
        sql_get_p = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}'
        self.prices = (high, open, close, low)
        readable_time = pd.to_datetime(time_of_event, unit='ms')
        columns = ['High_price', 'Open_price', 'Close_price', 'Low_price']
        data = pd.DataFrame(data=[self.prices], columns=columns, index=time_of_event)
        data.tail()

        sys_db.create_table(sybol, interval_of_candle)
        sys_db.commit()
        sys_db.save_to_db(sql_insert, self.prices)
        sys_db.commit()
        # d = sys_db.return_prices_df(operation=sql_get_p, columns=columns)
        # print(d)
        # macd = Signal(df=d).MACD()
        # mcad_sig = macd[-1]
        # eng = Signal(d).engulfing_small_time_interval()
        # last_signal = eng[-1]
        # print(mcad_sig)
        # push = pb.push_note(title='Bear reversal',
        #                     body=f'RSI value = {d}')
        #save close candlestick data
        if is_candle_close:
            sql_insert_closed = f'INSERT INTO {sybol}_{interval_of_candle}_closed (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
            sql_get_closed = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}_closed'
            sys_db.create_table_close_candlestick(sybol, interval_of_candle)
            sys_db.commit()
            sys_db.save_to_db(sql_insert_closed, self.prices)
            sys_db.commit()
            df = sys_db.return_prices_df(operation=sql_get_closed, columns=columns)

            sys_db.commit()
            # print(df)
            eng = Signal(df).engulfing_small_time_interval()
            last_signal = eng[-1]
            testing_rsi = Signal(df=df).RSI_Signal()
            last_rsi_value = testing_rsi[-1]
            tar = Signal(df).mytarget(int(0.0003))  # bars interms of seconds
            # print(tar[-1])
            # macd = Signal(df=df).MACD()
            # mcad_sig = macd[-1]
            # print(mcad_sig)
            if last_signal == 2 and last_rsi_value > 65:
                print("ENTERED OVERBOUGHT ZONE")
                push = pb.push_note(title='BULLISH ENGULFING | OVERBOUGHT ',
                                    body=f'RSI value = {last_rsi_value}\nPAIR HAS ENTERED A OVERBOUGHT ZONE')
            elif last_signal == 1 and last_rsi_value < 35:
                print("Bull signal")
                push = pb.push_note(title='BEARISH ENGULFING | OVERSOLD ',
                                    body=f'RSI value = {last_rsi_value}\nPAIR HAS ENTERED A OVERSOLD ZONE')
            elif last_signal == 0:
                print("NO actions required")
                print('rsi: ',last_rsi_value)
                print('signal ',last_signal)
                # print('macd ',mcad_sig)


    def _make_request(self, method: str, endpoint: str, data: typing.Dict) -> typing.List[Candle]:
        if method == "GET":
            try:
                response = requests.get(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:
                logger.error("CONNECTION ERROR WHILE MAKING %s REQUEST TO %s: %s", method, endpoint, e)
                return None
        elif method == "POST":
            try:
                response = requests.post(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:
                logger.error("CONNECTION ERROR WHILE MAKING %s REQUEST TO %s: %s", method, endpoint, e)
                return None
        elif method == "DELETE":
            try:
                response = requests.delete(self._base_url + endpoint, params=data, headers=self._headers)
            except Exception as e:
                logger.error("CONNECTION ERROR WHILE MAKING %s REQUEST TO %s: %s", method, endpoint, e)
                return None
        else:
            raise ValueError()

        if response.status_code == 200:
            return response.json()
        else:
            logger.error("ERROR ----> %s request to %s: % (error code %s)",
                         method, endpoint, response.status_code)
            return None

    # def _on_close(self, ws):
    #     logger.info("CONNECTION CLOSED")

    def streamData(self, symbol: str, timePeriod: str):
        userSocket = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'
        userWebSocket = websocket.WebSocketApp(url=userSocket, on_message=self._on_message)
        userWebSocketThread = threading.Thread(target=userWebSocket.run_forever)
        # userWebSocketThread.daemon = True
        pb.push_note(title='STRATEGY PLACED', body='OVER BOUGHT/SOLD HAS BEEN STARTED')
        userWebSocketThread.start()

    def _signature(self, data: typing.Dict):
        return hmac.new(self._apiSecret.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()

    def _price_action_analysis_on_webstream(self, ws: websocket, message):
        json_message = json.loads(message)
        candle = json_message['k']
        is_candle_close = candle['x']
        close = candle['c']
        high = candle['h']
        low = candle['l']
        vol = candle['v']
        open = candle['o']
        sybol = str(candle['s']).lower()
        interval_of_candle = str(candle['i'])
        num_of_trades = candle['n']
        time_of_event = [json_message['E']]
        sql_table = f'CREATE TABLE IF NOT EXISTS {sybol}_{interval_of_candle}(time BIGINT, High_price FLOAT(8,8), Open_price FLOAT(8,8), Close_price FLOAT(8,8), Low_price FLOAT(8,8), Volume DOUBLE(4,4))'
        sql_insert = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
        sql_insert_p = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price, Volume) VALUES (%s, %s, %s, %s, %s)'
        sql_get_all = f'SELECT * FROM {sybol}_{interval_of_candle}'
        sql_get = f'SELECT High_price, Open_price, Close_price, Low_price, Volume FROM {sybol}_{interval_of_candle}'
        sql_get_p = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}'
        self.prices = (high, open, close, low)
        readable_time = pd.to_datetime(time_of_event, unit='ms')
        columns = ['High_price', 'Open_price', 'Close_price', 'Low_price']
        data = pd.DataFrame(data=[self.prices], columns=columns, index=time_of_event)
        data.tail()

        if is_candle_close:
            sql_insert_closed = f'INSERT INTO {sybol}_{interval_of_candle}_closed (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
            sql_get_closed = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}_closed'
            sys_db.create_table_close_candlestick(sybol, interval_of_candle)
            sys_db.commit()
            sys_db.save_to_db(sql_insert_closed, self.prices)
            sys_db.commit()
            # fetching data in the form of a pd.DataFrame
            df = sys_db.return_prices_df(operation=sql_get_closed, columns=columns)
            sys_db.commit()
            # Technical Analysis part
            eng = Signal(df).engulfing_small_time_interval()
            last_signal = eng[-1]
            testing_rsi = Signal(df=df).RSI_Signal()
            last_rsi_value = testing_rsi[-1]
            tar = Signal(df=df).mytarget(3)  # bars interms of days
            trend = tar[-1]

            if last_signal == 2 and trend == 1 and last_rsi_value < 30:
                print("Bullish Reversal")
                pb.push_note(title='BULLISH REVERSAL', body=f'ENGULFING CANDLE SPOTTED\nRSI {last_rsi_value}')
            elif last_signal == 1 and trend == 2 and last_rsi_value > 70:
                print("bearish signal")
                pb.push_note(title='BEARISH REVERSAL', body=f'ENGULFING CANDLE SPOTTED\nRSI {last_rsi_value}')
            elif last_signal == 0:
                print("N0 actions required")

    def reversal_stream(self, symbol: str, timePeriod: str):
        userSocket = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'
        userWebSocket = websocket.WebSocketApp(userSocket, on_message=self._price_action_analysis_on_webstream)
        userWebSocketThread = threading.Thread(target=userWebSocket.run_forever)
        # userWebSocketThread.daemon = True
        pb.push_note(title='STRATEGY PLACED', body='ENGULFING REVERSAL STRATEGY HAS BEEN STARTED')
        userWebSocketThread.start()

    def yf_data(self, base_Symbol, against_Symbol, start, end, timeperiod):
        sql_table = f'CREATE TABLE IF NOT EXISTS {base_Symbol}{against_Symbol}_{timeperiod}_closed(High_price FLOAT(8,2), Open_price FLOAT(8,2), Close_price FLOAT(8,2), Low_price FLOAT(8,2))'
        sql_insert_closed = f'INSERT INTO {base_Symbol}{against_Symbol}_{timeperiod}_closed (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
        yf_df = yf.download(f'{base_Symbol}-{against_Symbol}', start=start, end=end, interval=timeperiod)
        yf_df.tail()
        yf_df.isna().sum()
        # yf_df.dropna()
        high = yf_df['High']
        open = yf_df['Open']
        close = yf_df['Close']
        low = yf_df['Low']
        self.prices = (high, open, close, low)
        columns = ['High', 'Open', 'Close', 'Low']
        # sys_db.any_required_operation(operation=sql_table)
        # sys_db.commit()
        # sys_db.save_to_db(operation=sql_insert_closed, price=self.prices)
        # sys_db.commit()

        return pd.DataFrame(data=yf_df)

    def englfing_pattern_backtest(self, df: pd.DataFrame):
        df['signal'] = Signal(df=df).engulfing2()
        sql_table = 'CREATE TABLE IF NOT EXISTS signals (at_time TIMESTAMP)'

        # sys_db.ML_create_table(operation=sql_table)
        # sys_db.commit()

        print("Bear_singals ")
        bear_sig = df[df['signal'] == 1].count()
        bear_in = df[df['signal'] == 1].index
        # arr_index = np.array(dtype=None, object=bear_in)
        # for index in bear_in:
        print(bear_sig)

        print("Bull_singals ")
        bull_sig = df[df['signal'] == 2].count()
        print(bull_sig)
        df['Trend'] = Signal(df=df).target(3)

        conditions = [(df['Trend'] == 1) & (df['signal'] == 1), (df['Trend'] == 2) & (df['signal'] == 2)]
        values = [1, 2]
        df['result'] = np.select(conditions, values)
        print("RESULT: ")
        print(df['result'])

        return df

    def doji_pattern_backtest(self, df: pd.DataFrame, bars: int):
        df['signal'] = Signal(df=df).target_doji_backtest(bars)

        print("Bear_singals ")
        bear_sig = df[df['signal'] == 1].count()
        bear_in = df[df['signal'] == 1].index

        print(bear_sig)

        print("Bull_singals ")
        bull_sig = df[df['signal'] == 2].count()
        print(bull_sig)
        df['Trend'] = Signal(df=df).target_doji_backtest(bars)
        # print(df['Trend'])

        conditions = [(df['Trend'] == 1) & (df['signal'] == 1), (df['Trend'] == 2) & (df['signal'] == 2)]
        values = [1, 2]
        df['result'] = np.select(conditions, values)
        print("RESULT: ")
        print(df['result'])

        return df

    def false_positives(self,df: pd.DataFrame, trendId: int):
        false_positives = df[(df['Trend'] != trendId) & (df['signal'] == trendId)]  # false positives
        print("false signals ")
        df_ = pd.DataFrame(data=false_positives)
        # index = df_[df_.index:]
        dfpl = df[300:]
        fig = go.Figure(data=[go.Candlestick(x=dfpl.index,
                                             open=dfpl['Open'],
                                             high=dfpl['High'],
                                             low=dfpl['Low'],
                                             close=dfpl['Close'])])
        fig.show()
        print(df_)

    def accuray_pattern(self, trendId: int, df: pd.DataFrame):
        print("ACCURACY: ")
        if trendId == 1:
            print(df[df['result'] == trendId].result.count() / df[df['signal'] == trendId].signal.count() * 100, "%")
            print("--------------------------------")
        elif trendId == 2:
            print(df[df['result'] == trendId].result.count() / df[df['signal'] == trendId].signal.count() * 100, "%")
            print("--------------------------------")
        elif trendId == 0:
            print(df[df['result'] == trendId].result.count() / df[df['signal'] == trendId].signal.count() * 100, "%")
            print("--------------------------------")
        elif trendId == 3:
            print(df[df['result'] == trendId].result.count() / df[df['signal'] == trendId].signal.count() * 100, "%")
            print("-------------------")

    def _on_message_dogi(self, ws: websocket, message):
        json_message = json.loads(message)
        candle = json_message['k']
        is_candle_close = candle['x']
        close = candle['c']
        high = candle['h']
        low = candle['l']
        vol = candle['v']
        open = candle['o']
        sybol = str(candle['s']).lower()
        interval_of_candle = str(candle['i'])
        num_of_trades = candle['n']
        time_of_event = [json_message['E']]
        sql_table = f'CREATE TABLE IF NOT EXISTS {sybol}_{interval_of_candle}(time BIGINT, High_price FLOAT(8,8), Open_price FLOAT(8,8), Close_price FLOAT(8,8), Low_price FLOAT(8,8), Volume DOUBLE(4,4))'
        sql_insert = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
        sql_insert_p = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price, Volume) VALUES (%s, %s, %s, %s, %s)'
        sql_get_all = f'SELECT * FROM {sybol}_{interval_of_candle}'
        sql_get = f'SELECT High_price, Open_price, Close_price, Low_price, Volume FROM {sybol}_{interval_of_candle}'
        sql_get_p = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}'
        self.prices = (high, open, close, low)
        readable_time = pd.to_datetime(time_of_event, unit='ms')
        columns = ['High_price', 'Open_price', 'Close_price', 'Low_price']
        data = pd.DataFrame(data=[self.prices], columns=columns, index=time_of_event)
        data.tail()

        # sys_db.create_table(sybol, interval_of_candle)
        # sys_db.commit()
        # sys_db.save_to_db(sql_insert, self.prices)
        # sys_db.commit()
        # d = sys_db.return_prices_df(operation=sql_get_p, columns=columns)
        # dogi = Signal(df=d).dogi_stream()
        # last_doji = dogi[-1]
        # print(last_doji)

        #save close candlestick data
        if is_candle_close:
            sql_insert_closed = f'INSERT INTO {sybol}_{interval_of_candle}_closed (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
            sql_get_closed = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}_closed'
            sys_db.create_table_close_candlestick(sybol, interval_of_candle)
            sys_db.commit()
            sys_db.save_to_db(sql_insert_closed, self.prices)
            sys_db.commit()
            df = sys_db.return_prices_df(operation=sql_get_closed, columns=columns)

            sys_db.commit()
            # print(df)
            doji = Signal(df).dogi_stream()
            last_signal_dogi = doji[-1]
            # print(last_signal_dogi)

            testing_rsi = Signal(df=df).RSI_Signal()
            last_rsi_value = testing_rsi[-1]
            if last_signal_dogi == 2 and last_rsi_value <= 30:
                print('BUY')
                pb.push_note(title='BULLISH REVERSAL', body=f'DOJI CANDLE SPOTTED\nRSI {last_rsi_value}')
            elif last_signal_dogi == 1 and last_rsi_value <= 70:
                print('SELL')
                pb.push_note(title='BEARISH REVERSAL', body=f'DOJI CANDLE SPOTTED\nRSI {last_rsi_value}')
            else:
                print('NO ACTION REQUIRED')

    def streamData_dogi(self, symbol: str, timePeriod: str):
        userSocket = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'
        userWebSocket = websocket.WebSocketApp(url=userSocket, on_message=self._on_message_dogi)
        userWebSocketThread = threading.Thread(target=userWebSocket.run_forever)
        pb.push_note(title='STRATEGY STARTED', body='DOJI spotter has been placed')
        # userWebSocketThread.daemon = True
        userWebSocketThread.start()

    def return_prices_csv(self, base_Symbol, against_Symbol, timeperiod):
        sql_get_closed = f'SELECT High, Open, Close, Low FROM {base_Symbol}{against_Symbol}_{timeperiod}_closed'
        columns = ['High', 'Open', 'Close', 'Low']
        csv = sys_db.return_prices_csv(operation=sql_get_closed, columns=columns)
        sys_db.commit()
        return csv

    def _perfect_reversal_analysis_on_webstream(self, ws: websocket, message):
        json_message = json.loads(message)
        candle = json_message['k']
        is_candle_close = candle['x']
        close = candle['c']
        high = candle['h']
        low = candle['l']
        vol = candle['v']
        open = candle['o']
        sybol = str(candle['s']).lower()
        interval_of_candle = str(candle['i'])
        num_of_trades = candle['n']
        time_of_event = [json_message['E']]
        sql_table = f'CREATE TABLE IF NOT EXISTS {sybol}_{interval_of_candle}(time BIGINT, High_price FLOAT(8,8), Open_price FLOAT(8,8), Close_price FLOAT(8,8), Low_price FLOAT(8,8), Volume DOUBLE(4,4))'
        sql_insert = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
        sql_insert_p = f'INSERT INTO {sybol}_{interval_of_candle} (High_price, Open_price, Close_price, Low_price, Volume) VALUES (%s, %s, %s, %s, %s)'
        sql_get_all = f'SELECT * FROM {sybol}_{interval_of_candle}'
        sql_get = f'SELECT High_price, Open_price, Close_price, Low_price, Volume FROM {sybol}_{interval_of_candle}'
        sql_get_p = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}'
        self.prices = (high, open, close, low)
        readable_time = pd.to_datetime(time_of_event, unit='ms')
        columns = ['High_price', 'Open_price', 'Close_price', 'Low_price']
        data = pd.DataFrame(data=[self.prices], columns=columns, index=time_of_event)
        data.tail()

        if is_candle_close:
            sql_insert_closed = f'INSERT INTO {sybol}_{interval_of_candle}_closed (High_price, Open_price, Close_price, Low_price) VALUES (%s, %s, %s, %s)'
            sql_get_closed = f'SELECT High_price, Open_price, Close_price, Low_price FROM {sybol}_{interval_of_candle}_closed'
            sys_db.create_table_close_candlestick(sybol, interval_of_candle)
            sys_db.commit()
            sys_db.save_to_db(sql_insert_closed, self.prices)
            sys_db.commit()
            # fetching data in the form of a pd.DataFrame
            df = sys_db.return_prices_df(operation=sql_get_closed, columns=columns)
            sys_db.commit()
            # Technical Analysis part
            eng = Signal(df).engulfing_small_time_interval()
            last_signal = eng[-1]
            testing_rsi = Signal(df=df).RSI_Signal()
            last_rsi_value = testing_rsi[-1]
            dogi = Signal(df).dogi_stream()
            if last_signal == 2 and dogi[-2] == 2 or dogi[-3] == 2 and last_rsi_value <= 30:
                print("Bullish Reversal")
                pb.push_note(title='BULLISH REVERSAL',
                             body=f'ENGULFING & DOJI CANDLE SPOTTED\nRSI {last_rsi_value}\nTIME {readable_time}')
            elif last_signal == 1 and dogi[-2] == 1 or dogi[-3] == 1 and last_rsi_value >= 70:
                print("bearish signal")
                pb.push_note(title='BEARISH REVERSAL', body=f'ENGULFING & DOJI CANDLE SPOTTED\nRSI {last_rsi_value}\nTIME {readable_time}')
            elif last_signal == 0:
                print("N0 actions required")

    def trend_confirm_stream(self, symbol: str, timePeriod: str):
        userSocket = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'
        userWebSocket = websocket.WebSocketApp(userSocket, on_message=self._perfect_reversal_analysis_on_webstream)
        userWebSocketThread = threading.Thread(target=userWebSocket.run_forever)
        pb.push_note(title='STRATEGY STARTED', body='DOJI + ENGULFING + RSI strategy started')
        # userWebSocketThread.daemon = True
        userWebSocketThread.start()
