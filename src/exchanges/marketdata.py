import pandas as pd
import numpy as np
import MetaTrader5 as mt5
import requests
import yfinance as yf

from pandas_datareader import data as pdr
from datetime import datetime
from bs4 import BeautifulSoup

from src.exchanges.interface_corretoras import Corretora

class marketdata(Corretora):
    """
    Used for requesting Binance with API key and API secret
    """
    client = ""
    api_key = ""
    api_secret = ""
    
    def __init__(self, secrets):
        _api_key = secrets["market.data"]["apiKey"]
        _api_secret = secrets["market.data"]["apiSecret"]
        self.api_key = str(_api_key) if _api_key is not None else ""
        self.api_secret = str(_api_secret) if _api_secret is not None else ""
        self.client = ""
        yf.pdr_override()
        _type= 'Market Data'
    
    def _get_type(self):
        return self._type
    
    def _get_client(self):
        return self.client
    
    def _format_amount(self, amount):
        return "{:0.0{}f}".format(amount, 5)

    def get_futures_historical_data(self, coin_pair, period = '1mo', unit='1d'):
        """
            period = indica quantos períodos vamos ter, por exemplo, 60 periodos, com gráfico de 1 minuto (caso unit = 1) 
            unit = tempo gráfico
        """
        return False
    
    def get_historical_data(self, coin_pair, period = '1y', unit='1d', start_date = None, end_date = None):
        """
        Get historical data from yfinance
        
        * coin_pair can be a list of tickers, like "SPY AAPL MSFT"
        * valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        * valid intervals(unit): 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo    
        """
        period = '1y' if period == None else period
        unit = '1d' if unit == None else unit
        if (start_date == None):
            data = pdr.get_data_yahoo(coin_pair, 
                                    period = period,
                                    interval = unit,
                                    threads = True)
        else:
            data = pdr.get_data_yahoo(coin_pair, start_date, end_date)
        df = pd.DataFrame(columns=['T','O','BV','C','H','L','V'])
        df['T'] = (pd.to_datetime(data.index, unit="ms")).values
        df['O'] = data['Open'].values.astype(float)
        df['BV'] = data['Volume'].values.astype(float)
        df['C'] = data['Adj Close'].values.astype(float)
        df['H'] = data['High'].values.astype(float)
        df['L'] = data['Low'].values.astype(float)
        df['V'] = data['Volume'].values.astype(float)
        return df.to_dict('records')
    
    def get_market_summary(self, coin_pair):
        return False

    def get_order(self, coin_pair, order_uuid):
        order_data_raw = self.client.get_order(symbol = self._format_coinpair(coin_pair), order_uuid = order_uuid)
        IsOpen = self._order_status_to_IsOpen.get(order_data_raw['status'])
        Type = str(order_data_raw['type']) + '_' + str(order_data_raw['side'])
        return {
                'success':True,
                'result': {
                    'IsOpen': IsOpen,
                    "Type": Type
                }
            }
    
    def sell_limit(self, coin_pair, quantity, price):
        order = self.client.order_limit_sell(symbol = self._format_coinpair(coin_pair),
                                             quantity = self._format_amount(quantity),
                                             price = price
                                            )
        if order['status'] == "FILLED":
            return {
                'success':True,
                'result': {
                    'uuid': order['orderId'],
                    "Exchange": coin_pair
                }
            }

    def get_markets(self):
        return pd.read_csv('database/tickers.csv')