import json
import logging
import os
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

class KafkaProducerSingleton:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            conf = {
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'python-producer',
                'acks': 'all',
                'compression.type': 'lz4'
            }
            try:
                cls._instance = Producer(conf)
            except Exception as e:
                logger.error("Failed to initialize Kafka Producer: %s", e)
                return None
        return cls._instance

def delivery_callback(err, msg):
    if err:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.debug("Message delivered to %s [%s]", msg.topic(), msg.partition())

def publish_event(topic: str, event_data: dict):
    producer = KafkaProducerSingleton.get_instance()
    if not producer:
        logger.warning("Kafka producer not available. Dropping event: %s", event_data)
        return

    try:
        producer.produce(
            topic,
            json.dumps(event_data).encode("utf-8"),
            callback=delivery_callback
        )
        producer.poll(0)  # non-blocking poll to trigger delivery reports
    except Exception as e:
        logger.error("Error publishing event to %s: %s", topic, e)

def flush_producer():
    producer = KafkaProducerSingleton.get_instance()
    if producer:
        producer.flush(timeout=5.0)
