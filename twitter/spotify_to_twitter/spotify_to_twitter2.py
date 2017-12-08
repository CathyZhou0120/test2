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
import simplejson




psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("SELECT max(insert_date)max_date  FROM new_release ")
max_date = cur.fetchall()

cur.execute("select distinct artist,song_name from new_release where insert_date >= %s",(max_date))
artist_list = cur.fetchall()
artist_list2 = []
for i in artist_list:
    artist_list2.append([i[0],i[1]])

consumer_key = 'aBwzAuBhDX0KShy4vaQtWlRAY'
consumer_secret = 'xNZUisl44guCKjJLC2IBnMIDsj9cbCQcrGiUrXNdtQHxbkkPmy'
access_token = '360211556-hsxBM1esTmrBjIDBTVSAr1LrAjF8tkKs5sD13lLk'
access_token_secret = '8YhFUDGUMHFFgtjoQRFe13DbR50ZhAibGFudnzfrwaDA7'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

artist_list3 = [[x.encode('UTF8') for x in l] for l in artist_list2]

artist_list4=[]
for i in artist_list3:
    artist_list4.append(' '.join(i))

class StdOutListener(StreamListener):

    def on_data(self, data):
        print (data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=artist_list4)



print(artist_list3)
