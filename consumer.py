from kafka import KafkaConsumer
from loguru import logger
import json
import os

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:30092")

consumer = KafkaConsumer(
    'test-topic',                           # topic name
    bootstrap_servers=[bootstrap_servers],   # must match advertised listener
    auto_offset_reset='earliest',           # read from beginning if no offset
    enable_auto_commit=True,                # auto-commit offsets
    group_id='my-group',                    # consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logger.info("Listening for messages...")

for message in consumer:
    logger.info(f"Received: {message.value}")
