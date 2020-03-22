import tweepy
from textblob import TextBlob
import csv
import time
import os

consumer_key = ''
consumer_secret=''

access_token=''
access_token_secret=''

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)

api = tweepy.API(auth)

try:
    os.remove('result.csv')
except:
    pass

csvFile = open('result.csv', 'w')

csvWriter = csv.writer(csvFile)
data = tweepy.Cursor(api.search,q = "bitcoin",since = "2017-11-06",until = "2017-11-07",lang = "en").items()
while True:
    try:
        tweet = data.next()
        if tweet.user.followers_count > 0:
            csvWriter.writerow([tweet.user.name.encode('utf-8', errors='ignore'),tweet.user.followers_count,tweet.created_at, tweet.text.encode('utf-8', errors='ignore'),tweet.id])

    except tweepy.TweepError:
        time.sleep(600)
        continue
    except StopIteration:
        break


csvFile.close()
