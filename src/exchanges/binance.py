import pandas as pd
import numpy as np
from binance.client import Client
from src.exchanges.interface_corretoras import Corretora

class Binance(Corretora):
    """
    Used for requesting Binance with API key and API secret
    """
    client = ""
    api_key = ""
    api_secret = ""
    
    def __init__(self, secrets):
        _api_key = secrets["binance"]["apiKey"]
        _api_secret = secrets["binance"]["apiSecret"]
        self.api_key = str(_api_key) if _api_key is not None else ""
        self.api_secret = str(_api_secret) if _api_secret is not None else ""
        self.client = Client(_api_key, _api_secret)

    binance_interval = {
        'oneMin': Client.KLINE_INTERVAL_1MINUTE,
        'fiveMin': Client.KLINE_INTERVAL_5MINUTE
    }
    _order_status_to_IsOpen = {
        'NEW':True,
        'PARTIALLY_FILLED':True,
        'FILLED':False,
        'CANCELED':False,
        'PENDING_CANCEL':False,
        'REJECTED':False,
        'EXPIRED':False
    }

    _type= 'Binance'
    def _get_type(self):
        return self._type
    
    def _get_client(self):
        return self.client
    
    def _format_amount(self, amount):
        return "{:0.0{}f}".format(amount, 5)

    def _format_coinpair(self, _coin_pair):
        return _coin_pair.replace("-","")

    def _get_start_str(self, _interval):
        return str(int(_interval)*2) + " minutes ago GMT-3"
    
    def get_futures_historical_data(self, coin_pair, period, unit):
        #self.client = Client(self.api_key, self.api_secret, testnet=True)
        df = self.client.futures_historical_klines(symbol=self._format_coinpair(coin_pair), 
                                          interval=self.binance_interval.get(unit), 
                                          start_str=self._get_start_str(period), 
                                          limit=period)
        df = pd.DataFrame(df, 
                          columns=["OpenTime","O","H","L","C","V","T","BV","NumberOfTrades","TakerBuyBAVolume","TakerBuyQAV", "ignore"]
                         )[["O","BV","C","H","L","T","V"]]
        df['T'] = (pd.to_datetime(df["T"], unit="ms")).values
        df['O'] = df[['O']].astype(float)
        df['BV'] = df[['BV']].astype(float)
        df['C'] = df[['C']].astype(float)
        df['H'] = df[['H']].astype(float)
        df['L'] = df[['L']].astype(float)
        df['V'] = df[['V']].astype(float)
        return df.to_dict('records')
    
    def get_historical_data(self, coin_pair, period, unit):
        """
        Get historical data from binance
        original format:
        [
                [
                    1499040000000,      // Open time
                    "0.01634790",       // Open
                    "0.80000000",       // High
                    "0.01575800",       // Low
                    "0.01577100",       // Close
                    "148976.11427815",  // Volume
                    1499644799999,      // Close time
                    "2434.19055334",    // Quote asset volume
                    308,                // Number of trades
                    "1756.87402397",    // Taker buy base asset volume
                    "28.46694368",      // Taker buy quote asset volume
                    "17928899.62484339" // Ignore.
              ]
            ]
        """

        df = self.client.get_historical_klines(self._format_coinpair(coin_pair), 
                                          self.binance_interval.get(unit), 
                                          self._get_start_str(period), 
                                          limit=period*2)
        df = pd.DataFrame(df, 
                          columns=["OpenTime","O","H","L","C","V","T","BV","NumberOfTrades","TakerBuyBAVolume","TakerBuyQAV", "ignore"]
                         )[["O","BV","C","H","L","T","V"]]
        df['T'] = (pd.to_datetime(df["T"], unit="ms")).values
        df['O'] = df[['O']].astype(float)
        df['BV'] = df[['BV']].astype(float)
        df['C'] = df[['C']].astype(float)
        df['H'] = df[['H']].astype(float)
        df['L'] = df[['L']].astype(float)
        df['V'] = df[['V']].astype(float)
        return df.to_dict('records')
    
    def get_market_summary(self, coin_pair):
        _json = self.client.get_ticker(symbol = self._format_coinpair(coin_pair))
        pre_json = {
            'success': True,
            'message':'',
            'result':[
                {
                    'MarketName': _json['symbol'],
                    'High': _json['highPrice'],
                    'Low': _json['lowPrice'],
                    'Volume': _json['volume'],
                    'Last': _json['symbol'],
                    'BaseVolume': _json['quoteVolume'],
                    'TimeStamp': str(pd.to_datetime(_json['closeTime'], unit="ms")),
                    'Bid': _json['bidPrice'],
                    'Ask': _json['askPrice'],
                    'OpenBuyOrders': '',
                    'OpenSellOrders': '',
                    'PrevDay': _json['prevClosePrice'],
                    'Created': ''
                }
            ]
        }
        return pre_json

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
        exc_info = self.client.get_exchange_info().get('symbols')
        markets = []
        for info in exc_info:
            markets.append(info.get('symbol'))
        return markets

    def _create_otoco_orders(self, _coin_pair, price, settings, positionSide = 'BOTH'):
        """
            Uma ordem OTOCO (One Trigger One Cancels the Other) consiste de uma ordem de compra 
            (limit=FUTURE_ORDER_TYPE_LIMIT, com quantity e price, ou FUTURE_ORDER_TYPE_MARKET para ordem a mercado, apenas com quantity),
            junto com uma ordem de stop e uma de take profit. o acompanhamento deve ser manual, por isso a limitação de 2 ordens abertas no loop da main.
            O positionSide pode ser BOTH, LONG ou SHORT - vou voltar aqui e refazer essa parte, pois perdi a configuração ideal desta função em um rebase
        """
        _percent_max_in_operation = float(settings.get('tradeParameters').get('percent_max_in_operation'))
        stopLoss = price * 1 + float(settings.get('tradeParameters').get('stop_loss'))
        takeProfit = price * (1 + int(settings.get('tradeParameters').get('leverage')) *  int(settings.get('tradeParameters').get('profit')))
        _usdt_balance = float([x for x in  self._get_client.futures_account_balance() if x.get('asset')=='USDT'][0].get('balance'))
        quantity = self._format_amount(_usdt_balance / (price*_percent_max_in_operation))
        
        order_limit = self._get_client.futures_create_order(
            symbol=_coin_pair, 
            side=Client.SIDE_BUY,
            type=Client.FUTURE_ORDER_TYPE_LIMIT, 
            quantity=quantity, price = price,
            isolated=True, positionSide=positionSide
        )
        order_sl = client.futures_create_order(
            symbol=_coin_pair, 
            side=Client.SIDE_SELL,  closePosition=True,
            type=Client.FUTURE_ORDER_TYPE_STOP_MARKET, 
            quantity=quantity, stopPrice=stopLoss, 
            positionSide=positionSide, 
            timeInForce=Client.TIME_IN_FORCE_GTC)
        order_tp = client.futures_create_order(
            symbol=_coin_pair, 
            side=Client.SIDE_SELL,  closePosition=True,
            type=Client.FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
            quantity=quantity, stopPrice=takeProfit, 
            positionSide=positionSide, 
            timeInForce=Client.TIME_IN_FORCE_GTC)
        return order_limit, order_sl, order_tp