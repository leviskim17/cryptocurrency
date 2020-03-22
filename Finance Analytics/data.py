import pyspark as spark
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,udf,monotonically_increasing_id,unix_timestamp,round,avg
import re

from pyspark.sql import SparkSession

sc = spark.SparkContext()
sql = spark.SQLContext(sc)

sc.setLogLevel("OFF")

#sparkSession = SparkSession.builder.appName("BaseServer").getOrCreate()
#TweetSDF = sparkSession.read.csv('hdfs://localhost/user/team14/tweets.csv')
#print('===========================================================================')
#print('Tweets counts : ', TweetSDF.count())

TweetPD = pd.read_csv('./meta/tweets.csv',error_bad_lines=False,engine = 'python',header = None) 
TweetSDF = sql.createDataFrame(TweetPD)
TweetSDF = TweetSDF.dropna()
print('===========================================================================')
print('Tweets counts : ', TweetSDF.count())

#sparkSession = SparkSession.builder.appName("BaseServer").getOrCreate()
#BtcSDF = sparkSession.read.csv('hdfs://localhost/user/team14/tweets.csv')
#print('===========================================================================')
print('Prices counts : ', BtcSDF.count())

BtcPD=pd.read_csv('./meta/price.csv',error_bad_lines=False,engine = 'python',header = None) 
BtcSDF = sql.createDataFrame(BtcPD) #creating pandas df and then changing it to pyspark df
print('===========================================================================')
print('Prices counts : ', BtcSDF.count())

TweetSDF.select(monotonically_increasing_id().alias("rowId"),"*")
TweetSDF = TweetSDF.withColumnRenamed('0', 'UserID')
TweetSDF = TweetSDF.withColumnRenamed('1', 'UserName')
TweetSDF = TweetSDF.withColumnRenamed('2', 'Followers')
TweetSDF = TweetSDF.withColumnRenamed('3', 'DateTime')
TweetSDF = TweetSDF.withColumnRenamed('4', 'Tweet')
TweetSDF.show(5)

BtcSDF = BtcSDF.withColumnRenamed('0', 'DateTime')
BtcSDF = BtcSDF.withColumnRenamed('1', 'Prices')
BtcSDF = BtcSDF.filter(BtcSDF.DateTime != 'Date')
BtcSDF.show(5)

#cleaning hashtags,urls,emojis....
import preprocessor as p 
def clean_tweet(input_str):
#    input_str = re.sub(r'RT', '', input_str)
#    p.set_options(p.OPT.URL, p.OPT.EMOJI,p.OPT.MENTION)
#    input_str = p.clean(input_str)
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", input_str).split())

clean_tweet_udf = udf(clean_tweet, StringType())
CleanTW = TweetSDF.withColumn('CleanedTweets', clean_tweet_udf(TweetSDF['Tweet']))
CleanTW.show(5)

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

def senti_score(sentence):
    snt = analyser.polarity_scores(sentence)
    return ([snt['neg'], snt['neu'], snt['pos'], snt['compound']])

senti_score_udf = udf(senti_score, ArrayType(FloatType()))
SentiTW = CleanTW.withColumn('p_neg', senti_score_udf(CleanTW['CleanedTweets'])[0])
SentiTW = SentiTW.withColumn('p_neu', senti_score_udf(CleanTW['CleanedTweets'])[1])
SentiTW = SentiTW.withColumn('p_pos', senti_score_udf(CleanTW['CleanedTweets'])[2])
SentiTW = SentiTW.withColumn('p_comp', senti_score_udf(CleanTW['CleanedTweets'])[3])
SentiTW.show(5)

#mcasting the strings(DateTime) of tweets
#def time_format(stri):  
#    dic = {'Nov':'11','Oct':'10'}
#    ans = ''
    #ans = stri[-4:]+'-'+ dic[stri[4:7]]+'-'+stri[8:19]
#    return ans

#time_format_udf = udf(time_format,StringType())
#DateTW = SentiTW.withColumn('DateTime_c', time_format_udf(SentiTW['DateTime']))
#CleanTW = CleanTW.withColumn("DateTime_casted", CleanTW['DateTime_c'].cast(TimestampType()))
#DateTW = SentiTW.withColumn("DateTime_casted", CleanTW['DateTime'].cast(TimestampType()))
#DateTW.show(5)

FinalTw = SentiTW.selectExpr("DateTime as Date_Time", "CleanedTweets as Tweets", "p_neg","p_neu","p_pos","p_comp")
FinalTw.show(5) #selecting necessary columns


# ## Pre-Processing Bitcoin dataframe
from datetime import datetime 
from dateutil import parser


"""def Btc_Time_format(input_str): #manipulating and casting the strings(DateTime) of BTC dataframe to timestamps
    input_str = re.sub(r'/17','', input_str)
    input_str = '2017-'+ input_str
    input_str = re.sub(r'/', '-', input_str)
    input_str += ':00'
    return input_str[:10]+""+input_str[10:]
    
func_udf = udf(Btc_Time_format, StringType())
BtcSDF = BtcSDF.withColumn('Cleaned_BTC_Time', func_udf(BtcSDF['DateTime']))
BtcSDF.show(5)"""

#CleandfBtc = BtcSDF.withColumn("Cleaned_BTC_Time_New",BtcSDF['Cleaned_BTC_Time'].cast(TimestampType()))
FinalBtc = BtcSDF.withColumn("castedDateTime",BtcSDF['DateTime'].cast(TimestampType()))
FinalBtc.show(5)
FinalBtc = FinalBtc.selectExpr("castedDateTime as Date_Time", "Prices")
FinalBtc = FinalBtc.withColumn("Prices",FinalBtc['Prices'].cast(DoubleType()))
FinalBtc.show(5)

FinalTw.printSchema()

FinalBtc.printSchema()
FinalBtc.count()

dt_truncated = ((round(unix_timestamp(col('Date_Time')) / 60) * 60).cast('timestamp'))
FinalTw = FinalTw.withColumn('dt_truncated', dt_truncated)
FinalTw = FinalTw.selectExpr("dt_truncated as Date_Time","Tweets","p_neg","p_neu","p_pos","p_comp")
FinalTw.show(5)

FinalTw.registerTempTable("TweetTable")
FinalTw_avg = sql.sql("SELECT Date_Time As DateTime,AVG(p_neg) as P_Neg,AVG(p_neu) as P_Neu,AVG(p_pos) as P_Pos,AVG(p_comp) as P_Comp FROM TweetTable GROUP BY Date_Time")
FinalTw_avg.show(5)

FinalTw_avg.count()
from pyspark.sql.functions import *
df_sort = FinalTw_avg.sort(asc("Date_Time"))
df_sort.show(5)

FinalTw_avg.registerTempTable("avgs")
FinalBtc.registerTempTable("prices")
results = sql.sql("SELECT DateTime, P_Neg, P_Neu, P_Pos, P_Comp, Prices FROM avgs JOIN prices ON avgs.DateTime = prices.Date_Time order by avgs.DateTime")
#results = results.selectExpr("DateTime","avg(polarity)","avg(subj)","avg(p_pos)","avg(p_neg)","Price") Use this line if you are using text blob package
results.show(5)

results.count()
results.repartition(1).write.csv("modeldata.csv")