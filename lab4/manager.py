from flask import Flask, jsonify
from kafka import KafkaProducer
import logging
import time
import json
import uuid
import os
import signal
import sys
import traceback

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def connect_to_kafka():
    max_retries = 10
    retry_delay = 15
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    logger.debug(f"Connecting to Kafka at {bootstrap_servers}")
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

try:
    time.sleep(30)
    producer = connect_to_kafka()
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None

@app.route('/health', methods=['GET'])
def health_check():
    logger.debug("Received health check request")
    return jsonify({'status': 'healthy'}), 200

@app.route('/generate', methods=['POST'])
def generate_task():
    logger.debug("Received POST request to /generate")
    if producer is None:
        logger.error("Kafka producer not initialized")
        return jsonify({'error': 'Kafka producer not available'}), 500
    try:
        task_id = str(uuid.uuid4())
        message = json.dumps({'task_id': task_id, 'timestamp': time.time()})
        key = task_id.encode('utf-8')
        logger.debug(f"Sending message to Kafka: {message}")
        producer.send('tasks', key=key, value=message.encode('utf-8'))
        producer.flush()
        logger.info(f" [x] Sent {message}")
        return jsonify({'task_id': task_id, 'status': 'Task sent to Kafka'})
    except Exception as e:
        logger.error(f"Error processing task: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}. Shutting down gracefully...")
    if producer:
        logger.debug("Closing Kafka producer")
        producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == '__main__':
    logger.info("Starting Flask application")
    app.run(host='0.0.0.0', port=5000)
