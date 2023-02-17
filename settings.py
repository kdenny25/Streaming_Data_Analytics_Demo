import os
from os.path import join,dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

PUBNUB_SUB_KEY = os.environ.get("pubnub_SubKey")
PUBNUB_PUB_KEY = os.environ.get("pubnub_PubKey")
PUBNUB_USER_ID = os.environ.get("pubnub_UerID")
PUBNUB_TWITTER = os.environ.get("pubnub_Twitter")
PUBNUB_STOCK = os.environ.get("pubnub_Stock")

AWS_ACCESS_KEY_ID = os.environ.get('awsAccessKeyId')
AWS_SECRET_KEY = os.environ.get('awsSecretKey')
