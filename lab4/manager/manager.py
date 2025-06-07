import time
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import os
import logging
import signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

shutdown_flag = False

def handle_shutdown_signal(signum, frame):
    global shutdown_flag
    logger.info(f"Received shutdown signal {signum}. Gracefully shutting down...")
    shutdown_flag = True

signal.signal(signal.SIGINT, handle_shutdown_signal)
signal.signal(signal.SIGTERM, handle_shutdown_signal)

KAFKA_BROKER = os.environ.get('KAFKA_BROKER_URL', 'kafka:9092')
TASKS_TOPIC = os.environ.get('KAFKA_TASKS_TOPIC', 'tasks-to-do')
RESULTS_TOPIC = os.environ.get('KAFKA_RESULTS_TOPIC', 'tasks-completed')
GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'my-worker-group')

def get_consumer():
    consumer = None
    retries = 5
    while retries > 0 and consumer is None:
        try:
            consumer = KafkaConsumer(
                TASKS_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            logger.info("Kafka Worker (Consumer) connected successfully.")
        except KafkaError as e:
            logger.error(f"Failed to connect Kafka Worker (Consumer): {e}. Retrying...")
            retries -= 1
            time.sleep(5)
    return consumer

def get_producer():
    producer = None
    retries = 5
    while retries > 0 and producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Kafka Worker (Producer for results) connected successfully.")
        except KafkaError as e:
            logger.error(f"Failed to connect Kafka Producer: {e}. Retrying...")
            retries -= 1
            time.sleep(5)
    return producer

if __name__ == "__main__":
    task_consumer = get_consumer()
    result_producer = get_producer()

    if not task_consumer or not result_producer:
        logger.error("Could not initialize Kafka connections for Worker. Exiting.")
        exit(1)

    logger.info(f"Worker subscribed to topic '{TASKS_TOPIC}' with group_id '{GROUP_ID}'")
    logger.info("Worker started. Press Ctrl+C to exit.")

    try:
        while not shutdown_flag:
            for partition, messages in task_consumer.poll(timeout_ms=1000).items():
                for message in messages:
                    if shutdown_flag:
                        break

                    task = message.value
                    logger.info(f"Received task: {task} (Partition: {message.partition}, Offset: {message.offset})")

                    time.sleep(1)

                    completion_message = {
                        'status': 'completed',
                        'original_task_id': task.get('task_id'),
                        'details': f"Task {task.get('task_id')} was processed by worker.",
                        'processed_at': time.time()
                    }

                    try:
                        future = result_producer.send(RESULTS_TOPIC, value=completion_message)
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Sent completion confirmation to topic '{record_metadata.topic}'")
                    except KafkaError as e:
                        logger.error(f"Error sending completion message: {e}")

                if shutdown_flag:
                    break

        logger.info("Shutdown flag is set, exiting loop.")

    finally:
        logger.info("Closing Kafka connections...")
        if result_producer:
            result_producer.close()
            logger.info("Worker's result producer closed.")
        if task_consumer:
            task_consumer.close()
            logger.info("Worker's task consumer closed.")
        logger.info("Worker has been shut down.")
