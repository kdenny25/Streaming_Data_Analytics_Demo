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
    .appName('stream_stock_data') \
    .getOrCreate()

schema = StructType([StructField('symbol', StringType(), True),
                     StructField('order_quantity', IntegerType(), True),
                     StructField('bid_price', FloatType(), True),
                     StructField('trade_type', StringType(), True),
                     StructField('timestamp', TimestampType(), True)])

df = spark \
    .readStream \
    .format('http') \
    .option('endpoint', 'data_stream') \
    .load()

print(df.isStreaming())
