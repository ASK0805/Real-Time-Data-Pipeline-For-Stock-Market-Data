from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json
from s3fs import S3FileSystem

consumer = KafkaConsumer("stock-market", bootstrap_servers = ["localhost:9092"], 
                         value_deserializer = lambda x:loads(x.decode('utf-8')))

s3 = S3FileSystem()

for count, i in enumerate(consumer):
    with s3.open("s3://stock-market-dataset-001/file_{}.json".format(count),"w") as file:
        json.dump(i.value, file)

