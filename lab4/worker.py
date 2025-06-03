import logging
import time
from kafka import KafkaConsumer
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
                group_id='worker_group'
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

if __name__ == '__main__':
    consumer = connect_to_kafka()
    for message in consumer:
        logger.info(f" [x] Received {message.value.decode('utf-8')}")
        time.sleep(1)
