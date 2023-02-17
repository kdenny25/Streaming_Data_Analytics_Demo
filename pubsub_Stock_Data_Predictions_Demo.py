from settings import PUBNUB_PUB_KEY, PUBNUB_USER_ID, PUBNUB_STOCK, PUBNUB_TWITTER
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory, PNOperationType
import socket
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from datetime import date
from tweet_processing import preprocessing, get_phrase

from googletrans import Translator, constants

from textblob import TextBlob

import re

import sklearn

pnconfig = PNConfiguration()

pnconfig.subscribe_key = PUBNUB_STOCK
pnconfig.publish_key = PUBNUB_PUB_KEY
pnconfig.user_id = PUBNUB_USER_ID
pubnub = PubNub(pnconfig)

def my_publish_callback(envelope, status):
    # check whether request successfully completed or not
    if not status.is_error():
        pass # message successfully published to specified channel.
    else:
        pass # Handle message publish error. Check 'category' property to find out possible issue
        # because of which request did fail.
        # Request can be resent using: [status retry];

class MySubscribeCallback(SubscribeCallback):
    def presence(self, pubnub, presence):
        pass # handle income presence data

    def status(self, pubnub, status):
        if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            pass # This event happens when radio / connectivity is lost

        elif status.category == PNStatusCategory.PNConnectedCategory:
            # Connect event. You can do stuff like publish, and know you'll get it.
            # or use the connected event to confirm you are subscribed for
            # UI / internal notifications, etc
            pass
        elif status.category == PNStatusCategory.PNReconnectedCategory:
            pass
            # Handle message decryption error. Probably client configured to
            # encrypt messages and on live data feed it received plain text.
        elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
            pass
            # Handle message decryption error. Probably client configured to
            # encryp messages and on live data feed it received plain text.

    def message(self, pubnub, message):
        # Handle new message stored in message.message
        msg = message.message
        if msg['order_quantity'] >= 500:
            new_row = pd.DataFrame([msg])
            print(new_row)

            df = pd.read_csv('./data/stock_data'+ str(date.today()) +'.csv')
            df = pd.concat([df, new_row])
            df.to_csv('./data/stock_data'+ str(date.today()) +'.csv', index=False)

new_df = pd.DataFrame(columns=['symbol', 'order_quantity', 'bid_price', 'trade_type', 'timestamp'])
new_df.to_csv('./data/stock_data'+ str(date.today()) +'.csv', index=False)

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels('pubnub-market-orders').execute()






