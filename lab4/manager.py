import logging
import time
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_kafka():
    max_retries = 10
    retry_delay = 15
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

if __name__ == '__main__':
    producer = connect_to_kafka()
    while True:
        message = f"Task {time.time()}"
        key = str(time.time()).encode('utf-8')  # Добавляем уникальный ключ
        producer.send('tasks', key=key, value=message.encode('utf-8'))
        logger.info(f" [x] Sent {message}")
        time.sleep(5)
