import json
import spotipy
import time
import pandas as pd
import numpy as np
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
import re
import pprint
import io
import psycopg2
import warnings
warnings.filterwarnings("ignore")
import datetime
import tweepy
from sqlalchemy import create_engine
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#from tweeter  import *
from time import time,ctime


conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("select distinct tweet_id,tweet_text from tweets_new_sub")
df = cur.fetchall()
df2 = []
for i in df:
    df2.append([i[0],i[1]])

df3 = pd.DataFrame(df2)
df3.columns = ['tweet_id','tweet_text']


print(df3)
