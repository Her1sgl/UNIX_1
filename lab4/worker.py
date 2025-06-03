from kafka import KafkaConsumer
import logging
import json
import random
import time
import os
import traceback

logging.basicConfig(level=logging.DEBUG)  # Увеличен уровень логирования
logger = logging.getLogger(__name__)

def connect_to_kafka():
    max_retries = 10
    retry_delay = 15
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    logger.debug(f"Connecting to Kafka at {bootstrap_servers}")
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id='worker_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=30000,  # Увеличено время сессии
                heartbeat_interval_ms=10000,  # Увеличен интервал heartbeat
                max_poll_interval_ms=600000  # Увеличен интервал опроса
            )
            consumer.subscribe(['tasks'])  # Явная подписка на топик
            logger.info("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after all retries")
                raise

try:
    time.sleep(60)  # Увеличена задержка для синхронизации
    consumer = connect_to_kafka()
except Exception as e:
    logger.error(f"Failed to initialize Kafka consumer: {traceback.format_exc()}")
    exit(1)

for message in consumer:
    try:
        logger.debug(f"Received message: {message}")
        task = json.loads(message.value.decode('utf-8'))
        task_id = message.key.decode('utf-8') if message.key else 'unknown'
        result = random.randint(1, 100)
        logger.info(f"Task {task_id} processed with result: {result}")
        time.sleep(1)  # Simulate processing
    except Exception as e:
        logger.error(f"Error processing message: {traceback.format_exc()}")
