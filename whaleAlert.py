import numpy as np
import pandas as pd
from whalealert.whalealert import WhaleAlert
whale = WhaleAlert()
import time
import datetime as dt
import sqlite3
import yfinance
import sqlalchemy

rest_base_url = 'https://api.whale-alert.io/v1'
start_time = int(time.time() - 600)
# start = dt.datetime(2019,1,1)
end = dt.datetime.now()

end_time = dt.datetime.now()
transaction_count_limit = 10
engine = sqlalchemy.create_engine('sqlite:///WhaleAlert.db')


class WhaleSearch():
    def __init__(self, single_symbol: str):
        self.symbol = single_symbol

    def search_transaction(self):
        transaction_count_limit = 50
        success, transactions, status = whale.get_transactions(start_time, api_key=api_key,
                                                               limit=transaction_count_limit)
        print("RECENT WHALE TRANSACTION")
        for transaction in transactions:
            if transaction['symbol'] == self.symbol:
                owner1 = transaction['from']
                owner2 = transaction['to']
                print("==========================================================")
                print(f"amount Of value {self.symbol} ", transaction['amount'], "\n value in USD ", transaction['amount_usd'],
                      "\n From ", owner1['owner_type'], " ", owner1['owner'],
                      "\nTo ", owner2['owner_type'], " ", owner2['owner'])






# {'blockchain': 'ethereum', 'symbol': 'ETH', 'id': '1636404313', 'transaction_type': 'transfer',
# 'hash': 'feaec7e12c186db4439b788769d86701c4823490e274152ce329c44719e3cd4f',
# 'from': {'address': '7aefd0b0681c27a4eceb0358e281580e859691ba', 'owner_type': 'unknown', 'owner': ''},
# 'to': {'address': 'c098b2a3aa256d2140208c3de6543aaef5cd3a94', 'owner_type': 'unknown', 'owner': ''},
# 'timestamp': 1624735044, 'amount': 565.9995, 'amount_usd': 1009703.4, 'transaction_count': 1}


# success, transactions, status = whale.get_transactions(start_time, api_key=api_key, limit=transaction_count_limit)
# x = transactions[1]
# df = pd.DataFrame(transactions)
# for transaction in transactions:
#     if transaction['symbol'] == 'ETH':
#         print(transaction)
#         print("amount Of value ",transaction['amount'], "\n value in USD " , transaction['amount_usd'])
#
# df.to_sql('Whales', engine, if_exists='append')
# prices_from_sql = pd.read_sql('Whales', engine)


