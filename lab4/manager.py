from flask import Flask, jsonify
from kafka import KafkaProducer
import logging
import time
import json
import uuid
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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

producer = connect_to_kafka()

@app.route('/generate', methods=['POST'])
def generate_task():
    task_id = str(uuid.uuid4())
    message = json.dumps({'task_id': task_id, 'timestamp': time.time()})
    key = task_id.encode('utf-8')
    producer.send('tasks', key=key, value=message.encode('utf-8'))
    producer.flush()
    logger.info(f" [x] Sent {message}")
    return jsonify({'task_id': task_id, 'status': 'Task sent to Kafka'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
