import tweepy
import csv
import time
import os
 
consumer_key = 'mz3ZdAsnFcIbrBm6THtRnQ6iZ'
consumer_secret='5TcIU4mjZ91zXyF479bjwYJYuAMdhajyWerb3UY5nodHF1LyxK'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
 
access_token='817040983137263616-yvPMkGiIpe63d5EwfXbURT0lsVqQEqo'
access_token_secret='GoTqbXgeJ3l6kN4S61rHbtjqS79iMR3HWjhaABqqJDeQx'
auth.set_access_token(access_token, access_token_secret)
 
api = tweepy.API(auth)

keyword = "bitcoin"

cursor = tweepy.Cursor(api.search, 
                       q=keyword,
                       since='2018-04-28',
                       count=100,
                       include_entities=True)

csvFile = open('tweets.csv', 'w')
csvWriter = csv.writer(csvFile)

for i, tweet in enumerate(cursor.items()):
    print("{}: {}".format(i, tweet.text)) # chcp 65001 <= the cli setting is required for handling various characters
    csvWriter.writerow([tweet.id,tweet.user.name.encode('utf-8', errors='ignore'),tweet.user.followers_count,tweet.created_at, tweet.text.encode('utf-8', errors='ignore')])

csvFile.close()