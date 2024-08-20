import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json

producer = KafkaProducer(bootstrap_servers = ["localhost:9092"], value_serializer = lambda m:dumps(m).encode('utf-8'))

df = pd.read_csv("indexData.csv")

while True:
    ab = df.sample(1).to_dict(orient = "records")[0]
    producer.send("stock-market", value=ab)
    sleep(1)


producer.flush()