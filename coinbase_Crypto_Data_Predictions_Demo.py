from websocket import create_connection
import simplejson as json
import pandas as pd
from datetime import date
import datetime as dt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

class CB_Stream_Data:
    def __init__(self) -> None:
        # url to websocket data stream
        self.url = 'wss://ws-feed.exchange.coinbase.com'
        # default products to list
        self.product_ids = ["ETH-USD"]


    def stream_data(self):
        # establish a connection to websock server
        ws = create_connection(self.url)
        # message to be sent to server
        subscription = {
                        "type": "subscribe",
                        "product_ids": self.product_ids,
                        "channels": ["ticker"]
                        }
        # send message
        ws.send(json.dumps(subscription))
        # stream the data
        while True:
            data = json.loads(ws.recv())
            yield data


columns = ['price', 'best_bid', 'best_bid_size', 'best_ask', 'best_ask_size', 'last_size', 'time', 'prediction']

df = pd.DataFrame(columns=columns)

s = CB_Stream_Data()
s.product_ids = ['ETH-USD']
data = s.stream_data()

initial_start = True;
# number of iterations before fitting/saving data and starting over
num_iterations = 101
iterations = 0
file_number = 0

LR = LinearRegression()

for value in data:
    values = [value.get(key) for key in columns[:7]]

    file_name = './data/crypto_data' + str(date.today()) +'_' + str(file_number) + '.csv'

    new_row = pd.DataFrame([values], columns=columns[:7])

    if new_row['time'][0] is not None:   
        new_row['time'] = new_row['time'].apply(lambda x: pd.Timestamp(x).timestamp())

    if initial_start is False and iterations <= num_iterations:
        x = new_row.iloc[:, 1:7]
        new_row['prediction'] = LR.predict(x)

    print(iterations)
    df = pd.concat([df, new_row], ignore_index=True)

    # handles initial stream of data
    if initial_start is True and iterations == num_iterations:
        initial_start = False
        iterations = 0
        # the first row is empty so we drop it.
        df = df.drop(labels=0, axis=0)
        #df['time'] = df['time'].apply(lambda x: pd.Timestamp(x).timestamp())

        # separate independent variables from dependent variable
        x = df.iloc[:, 1:7]
        y = df.iloc[:, 0]
        # fit the linear regression model
        LR.fit(x, y)
        # save dataframe to csv
        df.to_csv(file_name, index=False)
        file_number +=1
        df = pd.DataFrame(columns=columns)

    if initial_start is False and iterations == num_iterations:
        iterations = 0
        x = df.iloc[:, 1:7]
        y = df.iloc[:, 0]
        print(f'R-Squared : {LR.score(x, y)}')
        LR.fit(x, y)
        df.to_csv(file_name, index=False)
        file_number += 1
        df = pd.DataFrame(columns=columns)


    iterations +=1

