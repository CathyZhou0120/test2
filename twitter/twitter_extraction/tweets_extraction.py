from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import pandas as pd
import re
import numpy as np 
import psycopg2
from pandas.io import sql
from sqlalchemy import create_engine
import sys  
from backports import csv
import io

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

file = sys.argv[1]

conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor() 



tweets_data = []
tweets_file = open(file, "r") ### this is where i put in the file of collected tweets 
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue

tests2=pd.DataFrame(tweets_data)

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

def findWholeWord(w):
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

data2=[]

for i,j in enumerate(tweets['text']):
    if j is not np.nan:
        j = re.sub("[^a-zA-Z]", " ",j)
        if (findWholeWord('by')(j)  ) :
        #print(i,j, tweets['lang'].iloc[i],tweets['country'].iloc[i])
            data2.append([i,j, tweets['createdat'].iloc[i],
                           tweets['geolocation'].iloc[i],
                           tweets['source'].iloc[i],
                           tweets['hashtags'].iloc[i],
                           tweets['name'].iloc[i],
                           tweets['id'].iloc[i],
                           tweets['screen_name'].iloc[i],
                           tweets['description'].iloc[i],
                           tweets['followers_count'].iloc[i],
                           tweets['friends_count'].iloc[i],
                           tweets['listed_count'].iloc[i],
                           tweets['favourites_count'].iloc[i],
                           tweets['statuses_count'].iloc[i],
                           tweets['language'].iloc[i],
                           tweets['mention'].iloc[i],
                           tweets['location'].iloc[i],
                           tweets['text'].iloc[i]]
                    ) 

data3 = pd.Series(data2)
data_final=[]
for i,j in enumerate(data3):
    #print(i,j[1])
    text = str(j[1])
    #print(text)
    m = re.search('NowPlaying\s(.+?)\sby\s(.+?)\son\s(.+)', text)
    if m:
        data_final.append([m.group(1),m.group(2),m.group(3),j[1],
                         j[2],
                         j[3],j[4],j[5],j[6],j[7],j[8],j[9],j[10],j[11],j[12],j[13],j[14],j[15],j[16],j[17],j[18]])

data_final=pd.DataFrame(data_final)
data_final.columns=['song','artist','music_platform','text','createdat',
                        'geolocation','source','hashtags','name','id','screen_name','description','followers_count',
                        'friends_count','listed_count','favourites_count','statuses_count','language','mention','location','text_original']

#data_final2 = data_final[['song','artist','music_platform','text','createdat','geolocation','source','hashtags','name','id',
#'screen_name','followers_count',
 #                       'friends_count','listed_count','favourites_count','statuses_count','language','mention','location']]


data_final['description']=  data_final['description'].str.encode('utf-8')
data_final['song']=  data_final['song'].str.encode('utf-8')
data_final['artist']=  data_final['artist'].str.encode('utf-8')
data_final['music_platform']=  data_final['music_platform'].str.encode('utf-8')
data_final['text']=  data_final['text'].str.encode('utf-8')
data_final['createdat']=  data_final['createdat'].str.encode('utf-8')
data_final['geolocation']=  data_final['geolocation'].str.encode('utf-8')
data_final['source']=  data_final['source'].str.encode('utf-8')
data_final['hashtags']=  data_final['hashtags'].str.encode('utf-8')
data_final['name']=  data_final['name'].str.encode('utf-8')
data_final['screen_name']=  data_final['screen_name'].str.encode('utf-8')
data_final['language']=  data_final['language'].str.encode('utf-8')
data_final['mention']=  data_final['mention'].str.encode('utf-8')
data_final['location']=  data_final['location'].str.encode('utf-8')
data_final['text_original'] = data_final['text_original'].str.encode('utf-8')



engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
#data_final.to_csv('data_extraction.csv',index = False,sep='\t', encoding='utf-8')
data_final.to_sql('tweets_extraction', engine, if_exists='append')
#data_final2= data_final.encode('utf-8')

#test = data_final[['song']]
#test.to_sql('test2',engine,if_exists='append')
#data_final2 = pd.DataFrame()
#with io.open("data_extraction.csv", "r", encoding="utf-8") as my_file:
 #   data_final2= csv.reader(my_file)


print(data_final)
