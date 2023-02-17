# Streaming Data Analytics Demo

This is a small app to demonstrate how to setup a streaming pipeline to AWS Kinesis using Python. Data for
the first part of this app is from a csv file containing the familiar titanic data set. The data is streamed
to an AWS kinesis streaming server where it sits. 

The second part of this project twitter data is streamed from the Pub/Nub website where tweets are then 
translated and noun phrases are extracted for analysis. The results are stored in a csv.

The third part, stock data is streamed from the Pub/Nub website where it is filtered for top trading stocks
the results are stored in a csv.

In the fourth part the stock data is analyzed using sci-kit learn to predict future market orders.

https://www.pubnub.com/demos/real-time-data-streaming/

Note: There are several attempts to use pySpark with Kinesis and Kafka. These are failed attempts so please
disregard.

# File Descriptions:

kafka_producer.py - successfully streams data from a Pub/Nub http endpoint and sends it locally.



