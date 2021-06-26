import asyncio
import logging
import time
import requests
import hmac
import hashlib
from urllib.parse import urlencode
import websocket
import threading
import json
import typing
import pprint
from binance.client import Client
from Interfaces import *
from models import *

logger = logging.getLogger()

#FOR STORING DATA (list)
closes, highs, lows, opens = [], [], [], []
closeCandeleData = [opens, closes, highs, lows]
symbol = 'adausd'
timePeriod = '1m'

class BinanceSpotClient:
    def __init__(self,  apiKey: str, apiSecret: str, testnet: bool):

        if testnet:
            self.base_url = "https://testnet.binancefuture.com"
            self.wss_url =  "wss://stream.binancefuture.com/ws"
        else:
            self.base_url = "https://api.binance.com"
            self.wss_url = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'

        self._apiKey = apiKey
        self._apiSecret = apiSecret
        self.connection = Client(apiKey, apiSecret)
        self.open_orders = Client.get_open_orders
        self.prices = dict()

        self.logs = []

    def _add_new_log(self, message: str):
        logger.info("%s", message)
        self.logs.append({"log": message, "displayed": False})

    async def get_balance(self):
        info = await self.connection.get_account()
        print("BALANCE: ")
        bal = info['balances']
        if bal:
            for b in bal:
                if float(b['free']) > 0:
                    print(b)
        else:
            print("NO Balance")

    async def get_Open_orders(self):
        open_orders = await self.connection.get_open_orders()
        if open_orders:
                for orders in open_orders:
                    print("CURRENT OPEN ORDERS: ")
                    print(orders)
        else:
             print("No Open orders")

    async def get_order_history(self):
        orders = dict()
        re = self._make_request("GET","/api/v3/historicalTrades", orders)
        order_history = await self.connection.get_historical_trades()
        print("Orders History: ")
        for orders in re:
            print(orders)

    async def get_api_status(self):
        apiStat = await self.connection.get_account_api_trading_status()

        if apiStat['success']:
            print("BINANCE API STATUS \n",
                  " -------- > TRUE = WORKING | FALSE = NOT CONNECTED < ---------")
            print(apiStat['success'])
        else:
            print("NO CONNECTION ESTABLISHED")

    def _on_message(self, ws: websocket, message):
        json_message = json.loads(message)
        candle = json_message['k']
        is_candle_close = candle['x']
        close = candle['c']
        high = candle['h']
        low = candle['l']
        # vol = candle['v']
        open = candle['o']
        #save close candlestick data

        if is_candle_close:
            self.prices[is_candle_close] = {open,closes,high,low}
            print("=====================================")
            print("PERVIOUS CANDLESTICK")
            opens.append(float(open))
            closes.append(float(close))
            highs.append(float(high))
            lows.append(float(low))

            self._add_new_log(symbol + " " +
                              str(self.prices[is_candle_close]))

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

    def get_all_orders(self):
        data = dict()
        info_all_orders = self._make_request("GET","/api/v3/allOrders (HMAC SHA256)")


    def _on_close(self, ws):
        logger.error("CONNECTION CLOSED")

    def streamData(self, symbol: str, timePeriod: str):
        userSocket = f'wss://stream.binance.com:9443/ws/{symbol}@kline_{timePeriod}'
        userWebSocket = websocket.WebSocketApp(userSocket, on_message=self._on_message)
        userWebSocketThread = threading.Thread(target=userWebSocket.run_forever)
        userWebSocketThread.daemon = True
        userWebSocketThread.start()


    def _signature(self, data: typing.Dict):
        return hmac.new(self._apiSecret.encode(), urlencode(data).encode(), hashlib.sha256).hexdigest()
