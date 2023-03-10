import boto3
import pandas as pd
import time
import requests
from json import loads


# creat a client for aws
def create_client(service, region):
    return boto3.client(service, region_name=region)

# load the titanic data from csv file
def load_data(filename):
    df = pd.read_csv(filename)
    return df

# send data to Kinesis
def send_kinesis(kinesis_client, kinesis_stream_name, kinesis_shard_count, data):
    # records will be stored in this list
    kinesisRecords = []

    # get number of rows and columns from data
    (rows, columns) = data.shape
    currentBytes = 0
    rowCount = 0
    totalRowCount = rows
    sendKinesis = False
    shardCount = 0

    for _, row in data.iterrows():
        # joins values together with '|'
        values = '|'.join(str(value) for value in row)

        # encode the string to bytes
        encodedValues = bytes(values, 'utf-8')

        # create a dict object of each row
        kinesisRecord = {
            "Data": encodedValues,
            "PartitionKey": str(shardCount)
        }

        kinesisRecords.append(kinesisRecord)

        # number of bytes from the string
        stringBytes = len(values.encode('utf-8'))
        # running total of bytes
        currentBytes = currentBytes + stringBytes

        # check if ready to send
        if len(kinesisRecords) == 500:
            sendKinesis = True

        # if byte size over 50000, proceed
        if currentBytes > 50000:
            sendKinesis = True

        # if last record then send
        if rowCount == totalRowCount - 1:
            sendKinesis = True

        # if sendKinesis is True
        if sendKinesis == True:
            # put the records to kinesis
            kinesis_client.put_records(
                Records=kinesisRecords,
                StreamName=kinesis_stream_name
            )

            # reset values for next loop
            kinesisRecords = []
            sendKinesis = False
            currentBytes = 0

            # increment shard count
            shardCount = shardCount + 1

            # if max shard count then reset
            if shardCount > kinesis_shard_count:
                shardCount = 0

        # increment row counter
        rowCount += 1

    # log out how many records were pushed
    print('Total Records sent to Kinesis: {0}'.format(totalRowCount))

def main():
    # start timer
    start = time.time()

    # create kinesis client
    kinesis = create_client('kinesis', 'us-east-1')

    # api endpoint
    data_stream = 'https://pubsub.pubnub.com/stream/sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8/pubnub-market-orders/0/10000'

    # send data to kinesis data stream
    stream_name = 'CS367_Streaming_Data_Analytics'
    stream_shard_count = 1

    s = requests.Session()
    headers = {'symbol'}
    # GET request to data stream
    with s.get(data_stream, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                msg = line.decode()[2:-22]
                if (len(msg) < 200):
                    msg = pd.DataFrame([msg])
                    print(msg)
                    send_kinesis(kinesis, stream_name, stream_shard_count, msg)


    # load in data from csv
    #data = load_data('./data/titanic_train.csv')





    end = time.time()
    print('Runtime:' + str(end-start))

if __name__ == "__main__":
    # run main
    main()