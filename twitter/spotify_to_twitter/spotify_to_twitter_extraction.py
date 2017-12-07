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
import sys
from sqlalchemy import create_engine
import psycopg2
import sys  

reload(sys)  
sys.setdefaultencoding('utf8')



file = sys.argv[1]

tweets_data = []
tweets_file = open(file, "r") ### this is where i put in the file of collected tweets
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue

tests2=pd.DataFrame(tweets_data)

#print(tests2.columns)


tweets=pd.DataFrame()
tweets['createdat']= tests2['created_at']
tweets['text']= tests2['text']
tweets['geolocation']=tests2['geo']
tweets['source']= tests2['source']
#tweets['hashtag']=[i['text'] for i in tests2['entities'][0]['hashtags']]

location = []
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['location']!=[]:
            location.append(j['location'])
        else:
            location.append(0)
    else:
        location.append(0)

hashtags=[]
for i,j in enumerate(tests2['entities']):
    if j is not  np.nan:
        if j['hashtags']!=[]:
            hashtags.append(j['hashtags'][0]['text'])
        else:
            hashtags.append(0)
    else:
        hashtags.append(0)
tweets['hashtags']=hashtags

name = []
id_list=[]

for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['name'] !=[]:
            name.append(j['name'])
            id_list.append(j['id'])
        else:
            name.append(0)
            id_list.append(0)
    else:
        name.append(0)
        id_list.append(0)

screen_name = []
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['screen_name'] !=[]:
            screen_name.append(j['screen_name'])
        else:
            screen_name.append(0)
    else:
        screen_name.append(0)

description=[]
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['description'] !=[]:
            description.append(j['description'])
        else:
            description.append(0)
    else:
        description.append(0)

followers_count=[]
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['followers_count'] !=[]:
            followers_count.append(j['followers_count'])
        else:
            followers_count.append(0)
    else:
        followers_count.append(0)


friends_count=[]
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['friends_count'] !=[]:
            friends_count.append(j['friends_count'])
        else:
            friends_count.append(0)
    else:
        friends_count.append(0)

listed_count=[]
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['listed_count'] !=[]:
            listed_count.append(j['listed_count'])
        else:
            listed_count.append(0)
    else:
        listed_count.append(0)

favourites_count=[]
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['favourites_count'] !=[]:
            favourites_count.append(j['favourites_count'])
        else:
            favourites_count.append(0)
    else:
        favourites_count.append(0)

statuses_count=[]
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['statuses_count'] !=[]:
            statuses_count.append(j['statuses_count'])
        else:
            statuses_count.append(0)
    else:
        statuses_count.append(0)


lang = []
for i,j in enumerate(tests2['user']):
    if j is not np.nan:
        if j['lang'] !=[]:
            lang.append(j['lang'])
        else:
            lang.append(0)
    else:
        lang.append(0)

mention_list=[]
for i in (tests2['entities']):
    if i is not np.nan:
        mention_list.append([i['user_mentions']])
    else:
        mention_list.append(0)

tweets['location']= location
tweets['name']=name
tweets['id']=id_list
tweets['screen_name']=screen_name
tweets['description'] = description
tweets['followers_count']=followers_count
tweets['friends_count'] = friends_count
tweets['listed_count']=listed_count
tweets['favourites_count']=favourites_count
tweets['statuses_count']=statuses_count
tweets['language']=lang
tweets['mention']=mention_list

############ get the artist ########################

conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()


cur.execute("SELECT max(insert_date)max_date from new_release ")
max_date = cur.fetchall()

cur.execute("SELECT distinct artist,song_name,album_id from new_release where insert_date >=%s ", (max_date))
artist_list = cur.fetchall()
artist_list2 = []


for i in (artist_list):
    artist_list2.append([i[0],i[1],i[2]])


artist_list3 = pd.DataFrame(artist_list2)
artist_list3.columns = ['artist','song_name','album_id']

###########################################################3

tweets['text']=tweets['text'].str.lower()
tweets['text']=tweets['text'].str.encode('utf-8', errors='ignore')


artist_list3['artist']=artist_list3['artist'].str.lower()
artist_list3['artist']=artist_list3['artist'].str.encode('utf-8', errors='ignore')
artist_list3['song_name']= artist_list3['song_name'].str.lower()
artist_list3['song_name']= artist_list3['song_name'].str.encode('utf-8',errors='ignore')

#########################################################3#

result = []
for i in artist_list3['artist']:
    for j in tweets['text']:
        if str(i) in str(j):
            result.append([i,j])

result=pd.DataFrame(result)
result.columns=['artist','text']
result['text']=result['text'].str.lower()
#print(result['artist'].unique())

################ merge ######################################33

final = pd.DataFrame()
final = pd.merge( result,tweets, on=['text'])
final = pd.merge( final,artist_list3, on=['artist'], how='inner')

#print(final)
#final = final.drop_duplicates()
#print(final['artist'].unique())
#print(artist_list3['artist'].unique())

#print(final2.columns)
#for i in final.columns:
 #   final[i]=final[i].apply(bytes)

#print(final.columns)
#print(artist_list3.columns)




#final.drop_duplicates(keep="last",inplace=True)
#print((final.columns))
final['artist']=final['artist'].str.encode('utf-8', errors='ignore')
final['text']=final['text'].str.encode('utf-8', errors='ignore')
#final['createdat']=final['createdat'].str.encode('ascii', errors='ignore')
final['geolocation']=final['geolocation'].str.encode('utf-8', errors='ignore')
final['source']=final['source'].str.encode('utf-8', errors='ignore')
final['hashtags']=final['hashtags'].str.encode('utf-8', errors='ignore')
final['name']=final['name'].str.encode('utf-8', errors='ignore')
#final['id']=final['id'].str.encode('ascii', errors='ignore')
final['screen_name']=final['screen_name'].str.encode('utf-8', errors='ignore')
final['description']=final['description'].str.encode('utf-8', errors='ignore')
final['language']=final['language'].str.encode('utf-8', errors='ignore')
final['mention']=final['mention'].str.encode('utf-8', errors='ignore')
final['song_name']=final['song_name'].str.encode('utf-8', errors='ignore')
#final['artist_id']=final['artist_id'].str.encode('ascii', errors='ignore')
final['album_id']=final['album_id'].str.encode('utf-8', errors='ignore')
#final['available_market']=final['available_market'].str.encode('ascii', errors='ignore')
final['location']=final['location'].str.encode('utf-8', errors='ignore')


final = final[['screen_name','id','name','description','hashtags','text','createdat','artist',
'location','geolocation','language','source','followers_count','friends_count','listed_count','favourites_count','statuses_count','song_name','album_id']]

final2 = final.drop_duplicates()

#print(final['mention'])
print(len(final2))

#print(mention)
#print(final2)
conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')

final.to_sql('spotify_new_release_artist_tweets_extraction', engine, if_exists='append',index=False)

print('new release artist tweets extraction  file completed')








