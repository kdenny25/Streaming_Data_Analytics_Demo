from settings import PUBNUB_SUB_KEY, PUBNUB_PUB_KEY, PUBNUB_USER_ID, PUBNUB_TWITTER
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

pnconfig = PNConfiguration()

pnconfig.subscribe_key = PUBNUB_TWITTER
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
            #pubnub.publish().channel('pubnub-twitter').message('Hello World!').pn_async(my_publish_callback)
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
        if 'extended_tweet' in msg:
            msg = msg['extended_tweet']['full_text']+'t_end'
            print(msg)
        else:
            msg = msg['text']+'t_end'
            print(msg)

        msg = re.sub(r'http\S+', '', msg)
        msg = re.sub('@\w+', '', msg)
        msg = re.sub('#', '', msg)
        msg = re.sub('RT', '', msg)
        msg = re.sub(':', '', msg)

        translation = translator.translate(msg)
        msg = translation.text

        noun_phrase = TextBlob(msg).noun_phrases

        new_row = pd.DataFrame([[msg, noun_phrase]], columns=['text', 'Noun Phrase'])

        df = pd.read_csv('./data/twitter_data'+ str(date.today()) +'.csv')
        df = pd.concat([df, new_row])
        df.to_csv('./data/twitter_data'+ str(date.today()) +'.csv', index=False)



new_df = pd.DataFrame(columns=['text', 'Noun Phrase'])
new_df.to_csv('./data/twitter_data'+ str(date.today()) +'.csv', index=False)

translator = Translator()

pubnub.add_listener(MySubscribeCallback())
pubnub.subscribe().channels('pubnub-twitter').execute()

# spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
# emp_RDD = spark.sparkContext.emptyRDD()

# define schema for dataframe
# columns = StructType([StructField('text', StringType(), False), StructField('Noun Phrase', StringType(), False)])
# tweet_lines = spark.readStream.format('csv').schema(columns).option('header', True)\
#                     .option('maxFilesPerTrigger', 1)\
#                     .load('./data/twitter_data'+ str(date.today()) +'.csv')
#
# words = preprocessing(tweet_lines)
# words = get_phrase((words))
# words = words.repartition(1)
# query = words.writeStream\
#         .format('console')\
#         .outputMode('complete')\
#         .start()\
#         .awaitTermination()
# query = words.writeStream.format('csv')\
#                 .trigger(processingTime="10 seconds")\
#                 .option('checkpointLocation', 'checkpoint/')\
#                 .option('path', './data/')\
#                 .outputMode('append')\
#                 .start().awaitTermination()



