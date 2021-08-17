import numpy as np

from connectors.Sys_db import system_database
from connectors.requirement_keys import *
import yfinance
import pandas as pd
import datetime as dt
import pandas_ta as ta
import smtplib

from sqlalchemy import engine

symbol = 'adausdt'
timePeriod = '1m'

# to save correct signals
ML_db = system_database(host=mysql_host,user=mysql_user,password=mysql_pass,db='ml_data')
ML_db.commit()



# df1['RSI'] = ta.rsi(close=df1['close'])
#CLASS TAKES A DATAFRAME OF INFORMATION AND FUNCTIONS HELP
# DETERMINING THE PATTERN
class Signal:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def RSI_Signal(self):
        self.df['RSI'] = ta.rsi(self.df['Close_price'], length=20)
        return self.df['RSI'].values

    def SMA(self, len):
        self.df['SMA'] = ta.sma(self.df['Close_price'], length=len)
        return self.df['SMA'].values
###### configuring how to manually make this and use with engulfing candlestick
    def MACD(self):
        exp1 = self.df['Close_price'].ewm(span=12, adjust=False).mean()
        exp2 = self.df['Close_price'].ewm(span=26, adjust=False).mean()
        self.df['MACD'] = exp1 - exp2
        self.df['signal_line'] = self.df['MACD'].ewm(span=9, adjust=False).mean()
        if self.df['MACD'].values[-1] < self.df['signal_line'].values[-1]:
            print('Bear')
            signal = 1
        elif self.df['MACD'].values[-1] > self.df['signal_line'].values[-1]:
            print('bull')
            signal = 2
        return signal

    def engulfing_Big_Time_Intervals(self):
        length = len(self.df)
        high = list(self.df['High_price'])
        low = list(self.df['Low_price'])
        close = list(self.df['Close_price'])
        open = list(self.df['Open_price'])
        signal = [0] * length
        bodydiff = [0] * length
        sql_operation_table = 'CREATE TABLE IF NOT EXISTS signals_data (signal INT)'
        sql_insert = 'INSERT INTO signals_data (signal) VALUES (%s)'
        for row in range(1, length):
            bodydiff[row] = abs(open[row] - close[row])
            bodydiffmin = 0.003
            if (bodydiff[row] > bodydiffmin and bodydiff[row - 1] > bodydiffmin and
                    open[row - 1] < close[row - 1] and
                    open[row] > close[row] and
                    (open[row] - close[row - 1]) >= +0e-5
                    and close[row] < open[row - 1]):
                signal[row] = 1
            elif (bodydiff[row] > bodydiffmin and bodydiff[row - 1] > bodydiffmin and
                  open[row - 1] > close[row - 1] and
                  open[row] < close[row] and
                  (open[row] - close[row - 1]) <= -0e-5 and close[row] > open[row - 1]):
                signal[row] = 2
            else:
                signal[row] = 0

        return signal

    def engulfing2(self):
        length = len(self.df)
        high = list(self.df['High'])
        low = list(self.df['Low'])
        close = list(self.df['Close'])
        open = list(self.df['Open'])
        signal = [0] * length
        bodydiff = [0] * length
        sql_operation_table = 'CREATE TABLE IF NOT EXISTS signals_data (signal INT)'
        sql_insert = 'INSERT INTO signals_data (signal) VALUES (%s)'
        for row in range(1, length):
            bodydiff[row] = abs(open[row] - close[row])
            bodydiffmin = 0.003
            if (bodydiff[row] > bodydiffmin and bodydiff[row - 1] > bodydiffmin and
                open[row - 1] < close[row - 1] and
                open[row] > close[row] and
                    # open[row]>=close[row-1] and close[row]<open[row-1]):
                (open[row] - close[row - 1]) >= +0e-5
                    and close[row] < open[row - 1]):
                signal[row] = 1
            elif (bodydiff[row] > bodydiffmin and bodydiff[row - 1] > bodydiffmin and
                  open[row - 1] > close[row - 1] and
                  open[row] < close[row] and
                  # open[row]<=close[row-1] and close[row]>open[row-1]):
                  (open[row] - close[row - 1]) <= -0e-5 and close[row] > open[row - 1]):
                signal[row] = 2
            else:
                signal[row] = 0

        return signal

    def mytarget(self, barsfront):
        length = len(self.df)
        high = list(self.df['High_price'])
        low = list(self.df['Low_price'])
        close = list(self.df['Close_price'])
        open = list(self.df['Open_price'])
        trendcat = [None] * length
        sql_operation_table = 'CREATE TABLE IF NOT EXISTS trend_data (signal INT)'
        sql_insert = 'INSERT INTO signals_data (trend_data) VALUES (%s)'
        piplim = 300e-5

        for line in range(0, length - 1 - barsfront):
            for i in range(1, barsfront + 1):
                if ((high[line + i] - max(close[line], open[line])) > piplim) and (
                        (min(close[line], open[line]) - low[line + i]) > piplim):
                    trendcat[line] = 3  # no trend

                    break
                elif (min(close[line], open[line]) - low[line + i]) > piplim:
                    trendcat[line] = 1  # -1 downtrend

                    break
                elif (high[line + i] - max(close[line], open[line])) > piplim:
                    trendcat[line] = 2  # uptrend
                    break
                else:
                    trendcat[line] = 0  # no clear trend
        return trendcat

    def target(self, barsfront):
        length = len(self.df)
        high = list(self.df['High'])
        low = list(self.df['Low'])
        close = list(self.df['Close'])
        open = list(self.df['Open'])
        trendcat = [None] * length
        sql_operation_table = 'CREATE TABLE IF NOT EXISTS trend_data (signal INT)'
        sql_insert = 'INSERT INTO signals_data (trend_data) VALUES (%s)'
        piplim = 300e-5
        for line in range(0, length - 1 - barsfront):
            for i in range(1, barsfront + 1):
                if ((high[line + i] - max(close[line], open[line])) > piplim) and (
                        (min(close[line], open[line]) - low[line + i]) > piplim):
                    trendcat[line] = 3  # no trend

                    #return trendcat
                elif (min(close[line], open[line]) - low[line + i]) > piplim:
                    trendcat[line] = 1  # -1 downtrend
                    # index = int(line.__index__())
                    # ML_db.ML_create_table(operation=sql_operation_table)
                    # ML_db.any_required_operation(operation=sql_operation_table, index)
                    break
                elif (high[line + i] - max(close[line], open[line])) > piplim:
                    trendcat[line] = 2  # uptrend
                    break
                else:
                    trendcat[line] = 0  # no clear trend
        return trendcat

    def engulfing_small_time_interval(self):
        length = len(self.df)
        high = list(self.df['High_price'])
        low = list(self.df['Low_price'])
        close = list(self.df['Close_price'])
        open = list(self.df['Open_price'])
        signal = [0] * length
        bodydiff = [0] * length
        sql_operation_table = 'CREATE TABLE IF NOT EXISTS signals_data (signal INT)'
        sql_insert = 'INSERT INTO signals_data (signal) VALUES (%s)'
        for row in range(1, length):
            bodydiff[row] = abs(open[row] - close[row])
            bodydiffmin = 0.003
            if (bodydiff[row] > bodydiffmin and bodydiff[row - 1] > bodydiffmin and
                    open[row - 1] < close[row - 1] and
                    open[row] > close[row]
                    and close[row] < open[row - 1]):
                signal[row] = 1

            elif (bodydiff[row] > bodydiffmin and bodydiff[row - 1] > bodydiffmin and
                  open[row - 1] > close[row - 1] and
                  open[row] < close[row]
                  and close[row] > open[row - 1]):
                signal[row] = 2
            else:
                signal[row] = 0

        return signal

    def dogi_stream(self):
        self.df.reset_index(drop=True, inplace=True)
        self.df['RSI'] = ta.rsi(self.df['Close_price'], length=20)

        self.df.dropna()
        length = len(self.df)
        high = list(self.df['High_price'])
        open = list(self.df['Open_price'])
        close = list(self.df['Close_price'])
        low = list(self.df['Low_price'])
        # # volume = list(self.df['Volume'])
        signal = [0] * length
        highdiff = [0] * length
        lowdiff = [0] * length
        bodydiff = [0] * length
        ratio_1 = [0] * length
        ratio_2 = [0] * length

        for row in range(0, length):
            highdiff[row] = high[row] - max(open[row], close[row])
            bodydiff[row] = abs(open[row] - close[row])
            #random number 0.002
            if bodydiff[row] < 0.002:
                bodydiff[row] = 0.002
            lowdiff[row] = min(open[row], close[row]) - low[row]
            ratio_1 = highdiff[row] / bodydiff[row]
            ratio_2 = lowdiff[row] / bodydiff[row]
            #sell
            if (ratio_1>2.5 and lowdiff[row]<0.3*highdiff[row] and
                    bodydiff[row]>0.03 and self.df.RSI[row]>50 and self.df.RSI[row]<70):
                signal[row] = 1
                print('sell')
            #buy
            elif(ratio_2>2.5 and highdiff[row]<0.23*lowdiff[row] and
                 bodydiff[row]>0.03 and self.df.RSI[row]<55 and self.df.RSI[row]>30):
                signal[row] = 2
                print('buy')


            return signal

    # Target Shooting Star

    def target_doji_stream(self, barsupfront):
        length = len(self.df)
        high = list(self.df['high_price'])
        low = list(self.df['low_price'])
        close = list(self.df['close_price'])
        open = list(self.df['open_price'])
        datr = list(self.df['ATR'])
        trendcat = [0] * length

        for line in range(0, length - barsupfront - 1):
            valueOpenLow = 0
            valueOpenHigh = 0

            highdiff = high[line] - max(open[line], close[line])
            bodydiff = abs(open[line] - close[line])

            pipdiff = datr[line] * 1.  # highdiff*1.3 #for SL 400*1e-3
            if pipdiff < 1.1:
                pipdiff = 1.1

            SLTPRatio = 2.  # pipdiff*Ratio gives TP

            for i in range(1, barsupfront + 1):
                value1 = close[line] - low[line + i]
                value2 = close[line] - high[line + i]
                valueOpenLow = max(value1, valueOpenLow)
                valueOpenHigh = min(value2, valueOpenHigh)

                if ((valueOpenLow >= (SLTPRatio * pipdiff)) and (-valueOpenHigh < pipdiff)):
                    trendcat[line] = 1  # -1 downtrend
                    break
                elif ((valueOpenLow < pipdiff)) and (-valueOpenHigh >= (SLTPRatio * pipdiff)):
                    trendcat[line] = 2  # uptrend
                    break
                else:
                    trendcat[line] = 0  # no clear trend

        return trendcat

    def dogi_backtest(self):
        self.df.reset_index(drop=True, inplace=True)
        self.df['RSI'] = ta.rsi(self.df['Close'], length=20)
        self.df.dropna()

        length = len(self.df)
        high = list(self.df['High'])
        open = list(self.df['Open'])
        close = list(self.df['Close'])
        low = list(self.df['Low'])
        volume = list(self.df['Volume'])
        signal = [0] * length
        highdiff = [0] * length
        lowdiff = [0] * length
        bodydiff = [0] * length
        ratio_1 = [0] * length
        ratio_2 = [0] * length

        for row in range(0, length):
            highdiff[row] = high[row] - max(open[row], close[row])
            bodydiff[row] = abs(open[row] - close[row])
            # random number 0.002
            if bodydiff[row] < 0.002:
                bodydiff[row] = 0.002
            lowdiff[row] = min(open[row], close[row]) - low[row]
            ratio_1 = highdiff[row] / bodydiff[row]
            ratio_2 = lowdiff[row] / bodydiff[row]
            # sell
            if (ratio_1 > 2.5 and lowdiff[row] < 0.3 * highdiff[row] and
                    bodydiff[row] > 0.03 and self.df.RSI[row] > 50 and self.df.RSI[row] < 70):
                signal[row] = 1
                print('sell')
            # buy
            elif (ratio_2 > 2.5 and highdiff[row] < 0.23 * lowdiff[row] and
                  bodydiff[row] > 0.03 and self.df.RSI[row] < 55 and self.df.RSI[row] > 30):
                signal[row] = 2
                print('buy')

            return signal

    def target_doji_backtest(self, barsupfront):
        self.df['ATR'] = self.df.ta.atr(length=20)
        length = len(self.df)
        high = list(self.df['high'])
        low = list(self.df['low'])
        close = list(self.df['close'])
        open = list(self.df['open'])
        datr = list(self.df['ATR'])
        trendcat = [0] * length

        for line in range(0, length - barsupfront - 1):
            valueOpenLow = 0
            valueOpenHigh = 0

            highdiff = high[line] - max(open[line], close[line])
            bodydiff = abs(open[line] - close[line])

            pipdiff = datr[line] * 1.  # highdiff*1.3 #for SL 400*1e-3
            if pipdiff < 1.1:
                pipdiff = 1.1

            SLTPRatio = 2.  # pipdiff*Ratio gives TP

            for i in range(1, barsupfront + 1):
                value1 = close[line] - low[line + i]
                value2 = close[line] - high[line + i]
                valueOpenLow = max(value1, valueOpenLow)
                valueOpenHigh = min(value2, valueOpenHigh)

                if ((valueOpenLow >= (SLTPRatio * pipdiff)) and (-valueOpenHigh < pipdiff)):
                    trendcat[line] = 1  # -1 downtrend
                    break
                elif ((valueOpenLow < pipdiff)) and (-valueOpenHigh >= (SLTPRatio * pipdiff)):
                    trendcat[line] = 2  # uptrend
                    break
                else:
                    trendcat[line] = 0  # no clear trend

        return trendcat
