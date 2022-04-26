from pickletools import bytes1
import sys
import socket

from confluent_kafka import Consumer, KafkaException, KafkaError, Producer

def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))

conf_cons = {'bootstrap.servers': "localhost:29092,localhost:39092,localhost:49092",
        'group.id': "foo",
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'on_commit': commit_completed}

conf_prod = {'bootstrap.servers': "localhost:29092,localhost:39092,localhost:49092",
        'client.id': socket.gethostname()}

producer = Producer(conf_prod)
consumer = Consumer(conf_cons)

input_topics=['EAS026sentences']
output_topic='EAS026counts'

messages = {}
running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_msg(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def process_msg(msg):
    producer.produce(output_topic, key=msg.key(), value=str(len(str(msg.value()).split())-1), on_delivery=acked)

basic_consume_loop(consumer,input_topics)
shutdown()
