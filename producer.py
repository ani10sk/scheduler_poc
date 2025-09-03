import json
from kafka import KafkaProducer
import time
from loguru import logger
import os

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")


producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,   # must match advertised listener
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# producer.send("test-topic", {"msg": "Hello Kafka"})
# producer.flush()
# print("Message sent!")

def on_send_success(record):
    logger.info(record.topic)
    logger.info(record.partition)
    logger.info(record.offset)

def on_send_failure(error):
    logger.error(error)

for i in range(1000):
    producer.send(topic='test-topic', value={'test': i})
    logger.info(i)
    time.sleep(5)

producer.flush()