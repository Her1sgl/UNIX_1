from kafka import KafkaConsumer
import logging
import json
import random
import time
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_kafka():
    max_retries = 10
    retry_delay = 15
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'tasks',
                bootstrap_servers=bootstrap_servers,
                group_id='worker_group',
                auto_offset_reset='earliest'
            )
            logger.info("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

consumer = connect_to_kafka()

for message in consumer:
    task = json.loads(message.value.decode('utf-8'))
    task_id = message.key.decode('utf-8') if message.key else 'unknown'
    result = random.randint(1, 100)
    logger.info(f"Task {task_id} processed with result: {result}")
    time.sleep(1)  # Simulate processing
