from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext, StreamingListener
from pyspark.sql.types import *
from pyspark.sql import SQLContext

# sc = SparkContext()
# ssc = StreamingContext(sc, 10)

data_stream ='https://pubsub.pubnub.com/stream/sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8/pubnub-market-orders/0/10000'


spark = SparkSession \
    .builder \
    .appName('stream_crypto_data') \
    .getOrCreate()

schema = StructType([StructField('type', StringType()),
                     StructField('sequence', IntegerType()),
                     StructField('product_id', StringType()),
                     StructField('price', FloatType()),
                     StructField('open_24h', FloatType()),
                     StructField('volume_24h', FloatType()),
                     StructField('low_24h', FloatType()),
                     StructField('high_24h', FloatType()),
                     StructField('volume_30d', FloatType()),
                     StructField('best_bid', FloatType()),
                     StructField('best_bid_size', FloatType()),
                     StructField('best_ask', FloatType()),
                     StructField('best_ask_size', FloatType()),
                     StructField('side', StringType()),
                     StructField('time', DateType()),
                     StructField('trade_id', IntegerType()),
                     StructField('last_size', FloatType())])



df = spark \
    .readStream \
    .format('socket') \
    .option('host', 'wss://ws-feed.exchange.coinbase.com') \
    .option('port', '9090') \
    .load()

print(df.isStreaming)

value_df = df.select(from_json(col('value').cast('string'), schema).alias('value'))

