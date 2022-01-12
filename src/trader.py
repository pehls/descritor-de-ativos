from pandas.core.frame import DataFrame
import pydash as py_
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from src.indicators import Indicators

from src.messenger import Messenger
from src.database import Database
from src.logger import logger

class Trader(object):
    """
    Used for handling all trade functionality
    """
    operator = ""
    gen_indicators = ""
    def __init__(self, secrets, settings, operator):
        self.trade_params = settings["tradeParameters"]
        self.pause_params = settings["pauseParameters"]

        self.Messenger = Messenger(secrets, settings)
        self.Database = Database()
        self.operator = operator(secrets)
        self.gen_indicators = Indicators(settings)
    
    def get_operator(self):
        return self.operator
    
    def get_gen_indicators(self):
        return self.gen_indicators

    def initialise(self):
        """
        Fetch the initial coin pairs to track and to print the header line
        """
        try:
            if len(self.Database.app_data["coinPairs"]) < 1:
                self.Database.store_coin_pairs(self.get_markets("BTC"))
            self.Messenger.print_header(len(self.Database.app_data["coinPairs"]))
        except ConnectionError as exception:
            self.Messenger.print_error("connection", [], True)
            logger.exception(exception)
            exit()

    def get_current(self, coin_pair, item):
        """
        Get current item for a coin pair. ex of response:
        
        [{'MarketName': 'USD-BTC', 
        'High': 61800.0, 'Low': 56103.07, 
        'Volume': 736.7149564, 'Last': 61293.493, 
        'BaseVolume': 43744317.6799586, 
        'TimeStamp': '2021-03-13T23:37:29.19', 
        'Bid': 61296.24, 'Ask': 61318.407, 
        'OpenBuyOrders': 7273, 'OpenSellOrders': 1447, 
        'PrevDay': 57359.66, 'Created': '2018-05-31T13:24:40.77'}]
        """
        coin_summary = self.operator.get_market_summary(coin_pair)
        if not coin_summary["success"]:
            error_str = self.Messenger.print_error("coinMarket", [coin_pair])
            logger.error(error_str)
            return None
        return coin_summary["result"][0][item]
    
    def get_current_price(self, coin_pair, price_type):
        """
        Gets current market price for a coin pair
        :param coin_pair: Coin pair market to check (ex: BTC-ETH, BTC-FCT)
        :type coin_pair: str
        :param price_type: The type of price to get (one of: 'ask', 'bid')
        :type price_type: str

        :return: Coin pair's current market price
        :rtype: float
        """
        coin_summary = self.operator.get_market_summary(coin_pair)
        if not coin_summary["success"]:
            error_str = self.Messenger.print_error("coinMarket", [coin_pair])
            logger.error(error_str)
            return None
        if price_type == "ask":
            print(coin_summary)
            return coin_summary["result"][0]["Ask"]
        if price_type == "bid":
            print(coin_summary)
            return coin_summary["result"][0]["Bid"]
        return coin_summary["result"][0]["Last"]
    
    def get_historical_prices(self, coin_pair, period = None, unit = None, futures = False, macd_rsi=False):
        """
        Returns closing prices within a specified time frame for a coin pair

        :param coin_pair: String literal for the market (ex: BTC-LTC)
        :type coin_pair: str
        :param period: Number of periods to query
        :type period: int
        :param unit: Ticker interval (one of: 'oneMin', 'fiveMin', 'thirtyMin', 'hour', 'week', 'day', and 'month')
        :type unit: str

        :return: Array of historical prices, array of signals
        :rtype: list, list
        """
        
        import warnings
        warnings.filterwarnings("ignore")
        if (period != None):
            period*=2
        if (futures):
            df = self.operator.get_futures_historical_data(coin_pair, period, unit)
        else:
            df = self.operator.get_historical_data(coin_pair, period, unit)
        df = pd.DataFrame(df).rename(columns={
            'O':"Open", 
            "BV":"Base Volume",
            "C":"Close",
            "H":"High",
            "L":"Low",
            "T":"Datetime",
            "V":"Volume"
        })
        df.Datetime = pd.to_datetime(df.Datetime)
        df.Datetime = df.Datetime - timedelta(hours=3)
        df.Open = df.Open.astype(float)
        df.Close = df.Close.astype(float)
        df.High = df.High.astype(float)
        df.Low = df.Low.astype(float)
        df.Volume = df.Volume.astype(float)
        
        df, crossovers, ma_crossovers, bb_crossovers, hammers, suportes, resistencias, high_trend, low_trend, close_trend =  self.get_gen_indicators().gen_all(df, period, macd_rsi)
        cols_to_drop = ['Tend.Alta (Min d-2 > min(d-1))2', 'Tend.Alta (Min d-1 > min(d))','Tend.Baixa(Máx d-1 & d-2)', 'Tend.Baixa(Máx d & d-1)','Tend.Alta (Hist d-2 < histd-1)', 'Tend.Alta (Hist d-1 < histd)2',
       'MACD < 0', 'Tend.Baixa (Hist d & d-1)', 'Tend.Baixa (Hist d-2 & d-1)','date_id','Standard Deviation']
        #df = df.merge(crossovers[['Datetime','Sinal de Compra (MACD)','Sinal de Venda (MACD)']], how='left')
        return self._drop_columns(df, cols_to_drop), crossovers, ma_crossovers, bb_crossovers, hammers, suportes, resistencias, high_trend, low_trend, close_trend
    
    def _drop_columns(self, df, cols_to_drop = []):
        if (cols_to_drop != None):
            for col in cols_to_drop:
                try:
                    df = df.drop(col, axis=1)
                except:
                    pass
        return df

    def get_closing_prices(self, coin_pair, period, unit):
        """
        Returns closing prices within a specified time frame for a coin pair

        :param coin_pair: String literal for the market (ex: BTC-LTC)
        :type coin_pair: str
        :param period: Number of periods to query
        :type period: int
        :param unit: Ticker interval (one of: 'oneMin', 'fiveMin', 'thirtyMin', 'hour', 'week', 'day', and 'month')
        :type unit: str

        :return: Array of closing prices and dates
        :rtype: list, list
        """
        historical_data = self.operator.get_historical_data(coin_pair, period, unit)
        #print(historical_data)
        closing_prices = []
        for i in historical_data:
            closing_prices.append(i["C"])
        dates = []
        for i in historical_data:
            dates.append(i["T"])
        return closing_prices, pd.to_datetime(dates) - timedelta(hours=3)

    def get_order(self, coin_pair, order_uuid, trade_time_limit):
        """
        Used to get an order from Bittrex by it's UUID.
        First wait until the order is completed before retrieving it.
        If the order is not completed within trade_time_limit seconds, cancel it.

        :param order_uuid: The order's UUID
        :type order_uuid: str
        :param trade_time_limit: The time in seconds to wait fot the order before cancelling it
        :type trade_time_limit: float

        :return: Order object
        :rtype: dict
        """
        start_time = time.time()
        if (self.operator._get_type()=="Binance"):
            order_data = self.operator.get_order(coin_pair, order_uuid)
        else:
            order_data = self.operator.get_order(order_uuid)
        while time.time() - start_time <= trade_time_limit and order_data["result"]["IsOpen"]:
            time.sleep(10)
            order_data = self.operator.get_order(order_uuid)

        if order_data["result"]["IsOpen"]:
            error_str = self.Messenger.print_error(
                "order", [order_uuid, trade_time_limit, order_data["result"]["Exchange"]]
            )
            logger.error(error_str)
            if order_data["result"]["Type"] == "LIMIT_BUY":
                self.operator.cancel(order_uuid)

        return order_data

    def buy(self, coin_pair, quantity, price, stats, trade_time_limit=2):
        """
        Used to place a buy order to Bittrex. Wait until the order is completed.
        If the order is not filled within trade_time_limit minutes cancel it.

        :param coin_pair: String literal for the market (ex: BTC-LTC)
        :type coin_pair: str
        :param btc_quantity: The amount of BTC to buy with
        :type btc_quantity: float
        :param price: The price at which to buy
        :type price: float
        :param stats: The buy stats object
        :type stats: dict
        :param trade_time_limit: The time in minutes to wait fot the order before cancelling it
        :type trade_time_limit: float
        """
        self.get_operator().buy(coin_pair, quantity, price, trade_time_limit)

    def sell(self, coin_pair, quantity, price, stats, trade_time_limit=2):
        """
        Used to place a sell order to Bittrex. Wait until the order is completed.
        If the order is not filled within trade_time_limit minutes cancel it.

        :param coin_pair: String literal for the market (ex: BTC-LTC)
        :type coin_pair: str
        :param price: The price at which to buy
        :type price: float
        :param stats: The buy stats object
        :type stats: dict
        :param trade_time_limit: The time in minutes to wait fot the order before cancelling it
        :type trade_time_limit: float
        """
        self.get_operator().sell(coin_pair, quantity, price, trade_time_limit)

    def analyse_buys(self):
        """
        Analyse all the un-paused coin pairs for buy signals and apply buys
        """
        trade_len = len(self.Database.trades["trackedCoinPairs"])
        pause_trade_len = len(self.Database.app_data["pausedTrackedCoinPairs"])
        if (trade_len < 1 or pause_trade_len == trade_len) and trade_len < self.trade_params["buy"]["maxOpenTrades"]:
            for coin_pair in self.Database.app_data["coinPairs"]:
                self.buy_strategy(coin_pair)

    def analyse_sells(self):
        """
        Analyse all the un-paused tracked coin pairs for sell signals and apply sells
        """
        for coin_pair in self.Database.trades["trackedCoinPairs"]:
            if coin_pair not in self.Database.app_data["pausedTrackedCoinPairs"]:
                self.sell_strategy(coin_pair)

    def buy_strategy(self, coin_pair):
        """
        Applies the buy checks on the coin pair and handles the results appropriately

        :param coin_pair: Coin pair market to check (ex: BTC-ETH, BTC-FCT)
        :type coin_pair: str
        """
        if (len(self.Database.trades["trackedCoinPairs"]) >= self.trade_params["buy"]["maxOpenTrades"] or
                coin_pair in self.Database.trades["trackedCoinPairs"]):
            return
        rsi = self.calculate_rsi(coin_pair=coin_pair, period=14, unit=self.trade_params["tickerInterval"])
        day_volume = self.get_current_24hr_volume(coin_pair)
        current_buy_price = self.get_current_price(coin_pair, "ask")

        if rsi is None:
            return

        if self.check_buy_parameters(rsi, day_volume, current_buy_price):
            buy_stats = {
                "rsi": rsi,
                "24HrVolume": day_volume
            }
            self.buy(coin_pair, self.trade_params["buy"]["btcAmount"], current_buy_price, buy_stats)
        elif "buy" in self.pause_params and rsi >= self.pause_params["buy"]["rsiThreshold"] > 0:
            self.Messenger.print_pause(coin_pair, [rsi, day_volume], self.pause_params["buy"]["pauseTime"], "buy")
            self.Database.pause_buy(coin_pair)
        else:
            self.Messenger.print_no_buy(coin_pair, rsi, day_volume, current_buy_price)

    def sell_strategy(self, coin_pair):
        """
        Applies the sell checks on the coin pair and handles the results appropriately

        :param coin_pair: Coin pair market to check (ex: BTC-ETH, BTC-FCT)
        :type coin_pair: str
        """
        if (coin_pair in self.Database.app_data["pausedTrackedCoinPairs"] or
                coin_pair not in self.Database.trades["trackedCoinPairs"]):
            return
        rsi = self.calculate_rsi(coin_pair=coin_pair, period=14, unit=self.trade_params["tickerInterval"])
        current_sell_price = self.get_current_price(coin_pair, "bid")
        profit_margin = self.Database.get_profit_margin(coin_pair, current_sell_price)

        if rsi is None:
            return

        if self.check_sell_parameters(rsi, profit_margin):
            sell_stats = {
                "rsi": rsi,
                "profitMargin": profit_margin
            }
            self.sell(coin_pair, current_sell_price, sell_stats)
        elif "sell" in self.pause_params and profit_margin <= self.pause_params["sell"]["profitMarginThreshold"] < 0:
            self.Messenger.print_pause(coin_pair, [profit_margin, rsi], self.pause_params["sell"]["pauseTime"], "sell")
            self.Database.pause_sell(coin_pair)
        else:
            self.Messenger.print_no_sell(coin_pair, rsi, profit_margin, current_sell_price)

