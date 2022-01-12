import smtplib
import time
import pandas as pd
#from telegramclient import telegramClient
from termcolor import cprint
from math import floor, ceil
from src.logger import logger
from datetime import datetime, timedelta

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    ConversationHandler,
    CallbackContext,
)

try:
    import winsound
except ImportError:
    winsound = None


class Messenger(object):
    """
    Used for handling messaging functionality
    """

    def __init__(self, secrets, settings):
        self.gmail = False
        if "gmail" in secrets:
            self.gmail = True
            self.from_address = secrets["gmail"]["username"]
            self.to_address_list = secrets["gmail"]["addressList"]
            self.login = secrets["gmail"]["username"]
            self.password = secrets["gmail"]["password"]
            self.recipient_name = secrets["gmail"]["recipientName"]
            self.smtp_server_address = "smtp.gmail.com:587"

        self.telegram = False
        if "telegram" in secrets:
            self.telegram = True
            self.telegram_channel = secrets["telegram"]["channel"]
            self.telegram_client = Updater(secrets["telegram"]["token"])

        self.sound = False
        if "sound" in settings:
            self.sound = settings["sound"]

        self.header_str = "\nTracking {} Bittrex Markets\n"

        self.console_str = {
            "buy": {
                "pause": "Pause buy tracking on {} with a high RSI of {} and a 24 hour volume of {} {} for {} minutes.",
                "resume": "Resuming tracking on all {} markets.",
                "message": "Buy on {:<10}\t->\t\tRSI: {:>2}\t\t24 Hour Volume: {:>5} {}\t\tBuy Price: {:.8f}\t\tURL: {}"
            },
            "sell": {
                "pause": "Pause sell tracking on {} with a low profit margin of {}% and an RSI of {} for {} minutes.",
                "resume": "Resume sell tracking on {}.",
                "message": "Sell on {:<10}\t->\t\tRSI: {:>2}\t\tProfit Margin: {:>4}%\t\tSell Price: {:.8f}\t\tURL: {}",
                "previousMessage": ""
            }
        }

        self.email_str = {
            "buy": {
                "subject": "Crypto Bot: Buy on {} Market",
                "message": ("Howdy {},\n\nI've just bought {} {} on the {} market - which is currently valued at {} {}."
                            "\n\nThe market currently has an RSI of {} and a 24 hour market volume of {} {}."
                            "\n\nHere's a Bittrex URL: {}\n\nRegards,\nCrypto Bot")
            },
            "sell": {
                "subject": "Crypto Bot: Sell on {} Market",
                "message": ("Howdy {},\n\nI've just sold {} {} on the {} market - which is currently valued at {} {}."
                            "\n\nThe market currently has an RSI of {} and a {} of {}% was made.\n\n"
                            "Here's a Bittrex URL: {}\n\nRegards,\nCrypto Bot")
            }
        }

        self.telegram_str = {
            "buy": {
                "emoji": ":heavy_minus_sign:",
                "message": "*Buy on {}*\n>>>\n_RSI: *{}*_\n_24 Hour Volume: *{} {}*_"},
            "sell": {
                "profit_emoji": ":heavy_check_mark:",
                "loss_emoji": ":x:",
                "message": "*Sell on {}*\n>>>\n_RSI: *{}*_\n_Profit Margin: *{}%*_"
            },
            "balance": {
                "emoji": ":bell:",
                "header": "*User Balances*\n\n>>>",
                "subHeader": "\n• *{}*\n",
                "subHeaderUntracked": "\n• *{} _(Untracked)_*\n",
                "subHeaderTotal": "\n*_{}_* {}\n",
                "balance": ">_Balance_: *{} {}*\n",
                "btcValue": ">_BTC Value_: *{} BTC*\n"
            }
        }

    def send_email(self, subject, message):
        """
        Used to send an email from the account specified in the secrets.json file to the entire
        address list specified in the secrets.json file

        :param subject: Email subject
        :type subject: str
        :param message: Email content
        :type message: str

        :return: Errors received from the smtp server (if any)
        :rtype: dict
        """
        if not self.gmail:
            return

        header = "From: %s\n" % self.from_address
        header += "To: %s\n" % ",".join(self.to_address_list)
        header += "Subject: %s\n\n" % subject
        message = header + message

        server = smtplib.SMTP(self.smtp_server_address)
        server.starttls()
        server.login(self.login, self.password)
        errors = server.sendmail(self.from_address, self.to_address_list, message)
        server.quit()

        return errors

    def send_telegram(self, message):
        """
        Send telegram message to notify users

        :param message: The message to send on the telegram channel
        :type message: str
        """
        if not self.telegram:
            return

        self.telegram_client.message.reply_text(
            text=message
        )
    
    def _print(self, coin_pair, period, dados, crossovers, MACDs):
        coin_pair = coin_pair.replace("-","")
        dados = dados.tail(1).reset_index(drop=True)
        logger.warning('--------------------------------------------------------------------')
        logger.warning("Catch at "+str(datetime.now()))
        logger.warning("Last crossover at " + 
              str(crossovers.tail(1).Datetime[len(crossovers)-1]) +
              ": " + crossovers.tail(1).Signal[len(crossovers)-1] + " at $"+str(crossovers.tail(1).Price[len(crossovers)-1])
             )
        logger.warning("Last MACD invertion at "+str(MACDs.Datetime[0]) +
              ": " + MACDs.MACD_Signal[0]+ " at $" + str(MACDs.Close[0]))
        logger.warning(dados.tail(1)['RSI Signal'][len(dados)-1] + "! RSI = "+str(dados.tail(1).RSI[len(dados)-1]))
        if (dados.tail(1).Signal[len(dados)-1] != ""):
            logger.error('--------------------------------------------------------------------')
            logger.error('--------------------------------------------------------------------')
            _str_op = (dados.tail(1).Signal[len(dados)-1] + " at $" + str(dados.tail(1).Close[len(dados)-1]) + " w/ Strategy " + dados.tail(1).Strategy[len(dados)-1])
            #self.send_telegram(_str_op)
            logger.error(_str_op)
            logger.error('--------------------------------------------------------------------')
            logger.error('--------------------------------------------------------------------')
            dados = dados.append(pd.read_csv("./database/"+coin_pair+"_"+period +"_dados_trades.csv", sep=";", decimal=","))
            dados.to_csv("./database/"+coin_pair+"_"+period +"_dados_trades.csv", index=False, sep=";", decimal=",")
        #logger.error(error_str)
    
    def play_beep(self, frequency=1000, duration=1000):
        """
        Used to play a beep sound

        :param frequency: The frequency of the beep
        :type frequency: int
        :param duration: The duration of the beep
        :type duration: int
        """
        if not self.sound or winsound is None:
            return
        winsound.Beep(frequency, duration)

    def play_sw_theme(self):
        """
        Used to play the Star Wars theme song
        """
        self.play_beep(1046, 880)
        self.play_beep(1567, 880)
        self.play_beep(1396, 55)
        self.play_beep(1318, 55)
        self.play_beep(1174, 55)
        self.play_beep(2093, 880)

        time.sleep(0.3)

        self.play_beep(1567, 600)
        self.play_beep(1396, 55)
        self.play_beep(1318, 55)
        self.play_beep(1174, 55)
        self.play_beep(2093, 880)

        time.sleep(0.3)

        self.play_beep(1567, 600)
        self.play_beep(1396, 55)
        self.play_beep(1318, 55)
        self.play_beep(1396, 55)
        self.play_beep(1174, 880)

    def play_sw_imperial_march(self):
        """
        Used to play the Star Wars Imperial March song
        """
        self.play_beep(440, 500)
        self.play_beep(440, 500)
        self.play_beep(440, 500)

        self.play_beep(349, 375)
        self.play_beep(523, 150)
        self.play_beep(440, 600)

        self.play_beep(349, 375)
        self.play_beep(523, 150)
        self.play_beep(440, 1000)

        time.sleep(0.2)

        self.play_beep(659, 500)
        self.play_beep(659, 500)
        self.play_beep(659, 500)

        self.play_beep(698, 375)
        self.play_beep(523, 150)
        self.play_beep(415, 600)

        self.play_beep(349, 375)
        self.play_beep(523, 150)
        self.play_beep(440, 1000)
