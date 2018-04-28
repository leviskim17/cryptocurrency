from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def start():


    sconf=SparkConf()
    sconf.set('spark.cores.max' , 8)
    sc=SparkContext(appName='kafkaconsumer',conf=sconf)
    ssc=StreamingContext(sc,10)

    brokers="quickstart.cloudera:9092"
    topic='test'
    kafkaStreams = KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list": brokers})
    
    #result=kafkaStreams.map(lambda x:(x[0],1)).reduceByKey(lambda x, y: x + y)
    
    kafkaStreams.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)
    
    #result.pprint()

    ### after prepared well then spark will start to compute
    ssc.start()             
    ssc.awaitTermination()  

offsetRanges = []

def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print "%s %s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset,o.untilOffset-o.fromOffset)

if __name__ == '__main__':
    start()

