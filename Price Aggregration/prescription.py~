from kafka import SimpleConsumer, SimpleClient
from kafka import KafkaConsumer
from kafka import KafkaClient

group_name = "my-group"
topic_name = "price"

#kafka = KafkaClient('quickstart.cloudera:9092')

consumer = KafkaConsumer(topic_name, bootstrap_servers=['quickstart.cloudera:9092'])

print "Created consumer for group: [%s] and topic: [%s]" % (group_name, topic_name)
print "Waiting for messages..."

for msg in consumer:
    print msg
