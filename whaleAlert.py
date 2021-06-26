import numpy as np
import pandas as pd
from whalealert.whalealert import WhaleAlert
whale = WhaleAlert()
import time
import datetime as dt


rest_base_url = 'https://api.whale-alert.io/v1'
start_time = int(time.time() - 600)

end = dt.datetime.now()

end_time = dt.datetime.now()
transaction_count_limit = 10



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
