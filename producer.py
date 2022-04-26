import random
from time import sleep
from uuid import uuid4
from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "localhost:29092,localhost:39092,localhost:49092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

topic='EAS026sentences'

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))
for i in range(10):
    r = random.randint(0,20)
    val = r* 'sentence '
    #print(val)
    #print(len(str(val).split()))
    producer.produce(topic, key=str(uuid4), value=val, on_delivery=acked)
    producer.poll(1)
    sleep(0.05)