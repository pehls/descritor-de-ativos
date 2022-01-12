import pandas as pd
import numpy as np

class Corretora(object):
    """
    Used for implementing basic functions for exchanges
    """
    client = ""
    api_key = ""
    api_secret = ""
    _type= ''
    def __init__(self, secrets):
        pass

    def _get_type(self):
        return self._type
    
    def _get_client(self):
        return self.client
    
    def _format_amount(self, amount):
        return "{:0.0{}f}".format(amount, 5)

    def _format_coinpair(self, _coin_pair):
        pass

    def get_futures_historical_data(self, coin_pair, period, unit):
        pass
    
    def get_historical_data(self, coin_pair, period, unit):
        """
        Get historical data
        original format:
        [
                [
                    T - Open time,
                    O - Open Price,
                    BV - Base Volume,
                    C - Close price,
                    H - High price,
                    L - Low price,
                    V - Volume
              ]
            ]
        """
        pass
    
    def get_market_summary(self, coin_pair):
        """Format:
         {
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
        """
        pass

    def get_order(self, coin_pair, order_uuid):
        """
        Order Information, return format:
         {
                'success':True,
                'result': {
                    'IsOpen': IsOpen,
                    "Type": Type
                }
            }
        """
        pass
    
    def sell_limit(self, coin_pair, quantity, price):
        """
        Place a sell order, return format:
         {
                'success':True,
                'result': {
                    'uuid': order['orderId'],
                    "Exchange": coin_pair
                }
            }
        """
        pass

    def get_markets(self):
        """
        Get all symbols in the historic get data
        """

    def _create_otoco_orders(self, _coin_pair, price, settings, positionSide = 'BOTH'):
        """
            Uma ordem OTOCO (One Trigger One Cancels the Other) consiste de uma ordem de compra 
            (limit=FUTURE_ORDER_TYPE_LIMIT, com quantity e price, ou FUTURE_ORDER_TYPE_MARKET para ordem a mercado, apenas com quantity),
            junto com uma ordem de stop e uma de take profit. o acompanhamento deve ser manual, por isso a limitação de 2 ordens abertas no loop da main.
            O positionSide pode ser BOTH, LONG ou SHORT - vou voltar aqui e refazer essa parte, pois perdi a configuração ideal desta função em um rebase
        
        Na implementação pela binance, recuperamos algumas informações:
        ´
            _percent_max_in_operation = float(settings.get('tradeParameters').get('percent_max_in_operation'))
            stopLoss = price * 1 + float(settings.get('tradeParameters').get('stop_loss'))
            takeProfit = price * (1 + int(settings.get('tradeParameters').get('leverage')) *  int(settings.get('tradeParameters').get('profit')))
            _usdt_balance = float([x for x in  self._get_client.futures_account_balance() if x.get('asset')=='USDT'][0].get('balance'))
            quantity = self._format_amount(_usdt_balance / (price*_percent_max_in_operation))
        ´
        Utilizamos todas elas para um gerenciamento (pequeno) de risco, inserindo um take profit desejado e um stop loss:

        ´
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
        ´

        No fim, retornamos os ids das ordens, para gerenciamento futuro (retorno da função futures_create_order do client)
        ´
        return order_limit, order_sl, order_tp
        ´
        """
        pass