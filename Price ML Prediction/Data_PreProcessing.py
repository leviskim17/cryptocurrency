import pyspark as spark
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,udf,monotonically_increasing_id,unix_timestamp,round,avg
import re
sc = spark.SparkContext()
sql = spark.SQLContext(sc)


# ## Loading tweets dataset
Twdf1=pd.read_csv('/Users/mezai/home/PBDA/tweetsfinal1.csv',error_bad_lines=False,engine = 'python',header = None) 
TwDF = Twdf1


# ## Loading Bitcoin prices dataset 
BtcDF=pd.read_csv('/Users/mezai/home/PBDA/BitCoinPrice.csv',error_bad_lines=False,engine = 'python',header = None) 
FullDataTw=sql.createDataFrame(TwDF)
FullDataBtc=sql.createDataFrame(BtcDF) #creating pandas df and then changing it to pyspark df


# In[4]:
FullDataTw = FullDataTw.dropna()
#print(FullDataTw.count())
#print(FullDataBtc.count())

FullDataTw.select(monotonically_increasing_id().alias("rowId"),"*")
FullDataTw = FullDataTw.withColumnRenamed('0', 'DateTime')
FullDataTw = FullDataTw.withColumnRenamed('1', 'Tweet')
FullDataBtc = FullDataBtc.withColumnRenamed('0', 'DateTime')
FullDataBtc = FullDataBtc.withColumnRenamed('1', 'Price')
FullDataBtc = FullDataBtc.filter(FullDataBtc.DateTime != 'Date')


# ## Pre-Processing Twitter dataframe
Tw_samp = FullDataTw 

import preprocessor as p #cleaning each tweet using tweet-preprocessor like removing hashtags,urls,emojis....
def function_udf(input_str):
    input_str = re.sub(r'RT', '', input_str)
    p.set_options(p.OPT.URL, p.OPT.EMOJI,p.OPT.MENTION)
    input_str = p.clean(input_str)
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", input_str).split())
func_udf = udf(function_udf, StringType())
CleanDF = Tw_samp.withColumn('CleanedTweets', func_udf(Tw_samp['Tweet']))
CleanDF.show(3)


# ## Sentiment analysis using Text blob and Vader packages

# Textblob code
# from textblob import TextBlob  #passing cleaned tweets and getting a sentiment score for each tweet
# from textblob.sentiments import NaiveBayesAnalyzer
# def senti_score_udf(input_str):
#     PSanalysis = TextBlob(input_str)
#     analysis = TextBlob(input_str,analyzer=NaiveBayesAnalyzer())
#     polarity = PSanalysis.sentiment.polarity
#     subjectivity = PSanalysis.sentiment.subjectivity
#     classification = analysis.sentiment.classification
#     p_pos = analysis.sentiment.p_pos
#     p_neg = analysis.sentiment.p_neg
#     return [polarity,subjectivity,classification,p_pos,p_neg] #subjectivity, polarity, p_neg, classification
# func_udf2 = udf(senti_score_udf, ArrayType(FloatType()))
# CleanDF = CleanDF.withColumn('polarity', func_udf2(CleanDF['CleanedTweets'])[0])
# CleanDF = CleanDF.withColumn('subj', func_udf2(CleanDF['CleanedTweets'])[1])
# CleanDF = CleanDF.withColumn('class', func_udf2(CleanDF['CleanedTweets'])[2])
# CleanDF = CleanDF.withColumn('p_pos', func_udf2(CleanDF['CleanedTweets'])[3])
# CleanDF = CleanDF.withColumn('p_neg', func_udf2(CleanDF['CleanedTweets'])[4])
# CleanDF.show(3)

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

def senti_score_udf(sentence):
    snt = analyser.polarity_scores(sentence)
    return ([snt['neg'], snt['neu'], snt['pos'], snt['compound']])

func_udf2 = udf(senti_score_udf, ArrayType(FloatType()))
CleanDF = CleanDF.withColumn('p_neg', func_udf2(CleanDF['CleanedTweets'])[0])
CleanDF = CleanDF.withColumn('p_neu', func_udf2(CleanDF['CleanedTweets'])[1])
CleanDF = CleanDF.withColumn('p_pos', func_udf2(CleanDF['CleanedTweets'])[2])
CleanDF = CleanDF.withColumn('p_comp', func_udf2(CleanDF['CleanedTweets'])[3])
CleanDF.show(3)

#manipulating and casting the strings(DateTime) of tweets dataframe to timestamps
def Tw_Time_format(stri):  
    dic = {'Nov':'11','Oct':'10'}
    ans = ''
    ans += stri[-4:]+'-'+ dic[stri[4:7]]+'-'+stri[8:19]
    return ans

func_udf3 = udf(Tw_Time_format,StringType())
CleanDF = CleanDF.withColumn('DateTime_c', func_udf3(CleanDF['DateTime']))
CleanDF = CleanDF.withColumn("DateTime_casted",CleanDF['DateTime_c'].cast(TimestampType()))
CleanDF.show(3)

FinalTw = CleanDF.selectExpr("DateTime_casted as Date_Time", "CleanedTweets as Cleaned_Tweets", "p_neg","p_neu","p_pos","p_comp")
FinalTw.show(5) #selecting necessary columns


# ## Pre-Processing Bitcoin dataframe
from datetime import datetime 
from dateutil import parser

def Btc_Time_format(input_str): #manipulating and casting the strings(DateTime) of BTC dataframe to timestamps
    input_str = re.sub(r'/17','', input_str)
    input_str = '2017-'+ input_str
    input_str = re.sub(r'/', '-', input_str)
    input_str += ':00'
    return input_str[:10]+""+input_str[10:]
    
func_udf = udf(Btc_Time_format, StringType())
FullDataBtc = FullDataBtc.withColumn('Cleaned_BTC_Time', func_udf(FullDataBtc['DateTime']))
FullDataBtc.show(5)


# In[13]:
CleandfBtc = FullDataBtc.withColumn("Cleaned_BTC_Time_New",FullDataBtc['Cleaned_BTC_Time'].cast(TimestampType()))
FinalBtc = CleandfBtc.selectExpr("Cleaned_BTC_Time_New as Date_Time", "Price")
FinalBtc = FinalBtc.withColumn("Price",FinalBtc['Price'].cast(DoubleType()))
FinalBtc.show(5)#In this cell, casting to timesstamp, changing col names and casting price type to double


# ## Dataframes Look like this...
FinalTw.printSchema()


# In[15]:
FinalBtc.printSchema()
FinalBtc.count()


# ## Truncating timestamps to hours and then grouping them by hour
dt_truncated = ((round(unix_timestamp(col('Date_Time')) / 3600) * 3600).cast('timestamp'))
FinalTw = FinalTw.withColumn('dt_truncated', dt_truncated)
FinalTw = FinalTw.selectExpr("dt_truncated as Date_Time","Cleaned_Tweets","p_neg","p_neu","p_pos","p_comp")
UTC = ((unix_timestamp(col('Date_Time'))+ 5*60*60).cast('timestamp'))
FinalTw = FinalTw.withColumn('UTC', UTC)
FinalTw = FinalTw.selectExpr("UTC as Date_Time","Cleaned_Tweets","p_neg","p_neu","p_pos","p_comp")
FinalTw.show(5)


# In[17]:
FinalTw.registerTempTable("temp")
FinalTw_avg = sql.sql("SELECT Date_Time As DateTime,AVG(p_neg) as P_Neg,AVG(p_neu) as P_Neu,AVG(p_pos) as P_Pos,AVG(p_comp) as P_Comp FROM temp GROUP BY Date_Time")
#FinalTw_avg = FinalTw.select("Date_Time","polarity","subj","p_pos","p_neg").groupBy("Date_Time").agg(avg(col("polarity","subj","p_pos","p_neg")))
FinalTw_avg.show(5)

#This cell is just to collect all the corpus per hour(for the future work)
from pyspark.sql import functions as f
df_with_text = FinalTw.groupby("Date_Time").agg(f.concat_ws(" ", f.collect_list(FinalTw.Cleaned_Tweets)))
df_with_text.show(5)


# In[30]:
FinalTw_avg.count()
from pyspark.sql.functions import *
df_sort = FinalTw_avg.sort(asc("Date_Time"))
df_sort.show(5)


# ## Joining twitter and bitcoin dataframes by DateTime
FinalTw_avg.registerTempTable("avgs")
FinalBtc.registerTempTable("prices")
results = sql.sql("SELECT DateTime, P_Neg, P_Neu, P_Pos, P_Comp, Price FROM avgs JOIN prices ON avgs.DateTime = prices.Date_Time order by avgs.DateTime")
#results = results.selectExpr("DateTime","avg(polarity)","avg(subj)","avg(p_pos)","avg(p_neg)","Price") Use this line if you are using text blob package
results.show(5)


# In[21]:
results.count()  # Final size of dataset after joining bitcoin and twitter dataframes


# In[23]:
results.repartition(1).write.csv("DataforModelExec.csv") #this will write df to single csv instead of writing diff csv acc to partitions 


# In[ ]:
# Now refer to LSTM notebook for the timeseries analysis

