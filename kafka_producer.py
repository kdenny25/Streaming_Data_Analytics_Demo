from kafka import KafkaProducer
from json import dumps, loads
import requests
from time import sleep

# initializing Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(2,0,2)
)

# api endpoint
data_stream ='https://pubsub.pubnub.com/stream/sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8/pubnub-market-orders/0/10000'

s = requests.Session()
headers = {'symbol'}
# GET request to data stream
with s.get(data_stream, stream=True) as resp:
    for line in resp.iter_lines():
        if line:
            msg = line.decode()[2:-22]
            if(len(msg) < 200):
                msg = loads(msg)
                print(msg)
                producer.send('stock_trades', value=msg)
