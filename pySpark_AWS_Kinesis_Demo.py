from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

#from settings import AWS_SECRET_KEY, AWS_ACCESS_KEY_ID
import os


# spark = SparkSession \
#     .builder \
#     .appName('stream_stock_data') \
#     .getOrCreate()

appName = 'stream_stock_data'
stream_name = 'CS367_Streaming_Data_Analytics'
region = 'us-east-1'
endpointUrl = 'kinesis.us-east-1.amazonaws.com'

sc = SparkContext(appName=appName)
ssc = StreamingContext(sc, 1)

lines = KinesisUtils.createStream(ssc=ssc
                                  , kinesisAppName=appName
                                  , streamName=stream_name
                                  , endpointUrl=endpointUrl
                                  , regionName=region
                                  , initialPositionInStream=InitialPositionInStream.TRIM_HORIZON
                                  , checkpointInterval=5)

lines.pprint()

# kinesis = spark \
#         .readStream \
#         .format('kinesis')\
#         .option('streamName', stream_name) \
#         .option('regionName', region) \
#         .option('endpointUrl', endpointUrl)\
#         .option('initialPosition', 'TRIM_HORIZON')\
#         .load()

schema = StructType([StructField('symbol', StringType(), True),
                     StructField('order_quantity', IntegerType(), True),
                     StructField('bid_price', FloatType(), True),
                     StructField('trade_type', StringType(), True),
                     StructField('timestamp', TimestampType(), True)])

# kinesis\
#     .selectExpr('CAST(data AS STRING)')\
#     .select(from_json('data', schema).alias('data'))\
#     .select('data.*')\
#     .writeStream\
#     .outputMode('append')\
#     .format('console')\
#     .trigger(once=True)\
#     .start()\
#     .awaitTermination()