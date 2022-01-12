from src.logger import logger
from src.directory_utilities import get_json_from_file
from src.database import Database
import utils.utils
from datetime import datetime, timedelta

import plotly.graph_objects as go

from plotly.subplots import make_subplots

import logging
import pandas as pd
import numpy as np

def get_secrets():
    secrets_file_directory = "./database/secrets.json"
    secrets_template = {
        "bittrex": {
            "bittrexKey": "BITTREX_API_KEY",
            "bittrexSecret": "BITTREX_SECRET"
        },
        "gmail": {
            "recipientName": "Folks",
            "addressList": [
                "EXAMPLE_RECIPIENT_1@GMAIL.COM",
                "EXAMPLE_RECIPIENT_2@GMAIL.COM",
                "ETC..."
            ],
            "username": "EXAMPLE_EMAIL@GMAIL.COM",
            "password": "GMAIL_PASSWORD"
        },
        "telegram": {
            "channel": "telegram_CHANNEL",
            "token": "telegram_TOKEN"
        }
    }
    secrets_content = get_json_from_file(secrets_file_directory, secrets_template)
    if secrets_content == secrets_template:
        print("Please completed the `secrets.json` file in your `database` directory")
        exit()

    return secrets_content

def get_settings():
    settings_file_directory = "./database/settings.json"
    settings_template = {
        "sound": False,
        "tradeParameters": {
            "tickerInterval": "TICKER_INTERVAL",
            "buy": {
                "btcAmount": 0,
                "rsiThreshold": 0,
                "24HourVolumeThreshold": 0,
                "minimumUnitPrice": 0,
                "maxOpenTrades": 0
            },
            "sell": {
                "lossMarginThreshold": 0,
                "rsiThreshold": 0,
                "minProfitMarginThreshold": 0,
                "profitMarginThreshold": 0
            }
        },
        "pauseParameters": {
            "buy": {
                "rsiThreshold": 0,
                "pauseTime": 0
            },
            "sell": {
                "profitMarginThreshold": 0,
                "pauseTime": 0
            },
            "balance": {
                "pauseTime": 0
            }
        }
    }
    settings_content = get_json_from_file(settings_file_directory, settings_template)
    if settings_content == settings_template:
        print("Please completed the `settings.json` file in your `database` directory")
        #exit()
        return settings_content

    return settings_content
