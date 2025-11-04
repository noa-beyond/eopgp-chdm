"""
Simple kafka producer schema
"""

import json
from kafka import KafkaProducer as k_KafkaProducer

from noachdm import messaging as noa_messaging


class KafkaProducer(noa_messaging.AbstractProducer):
    """
    Kafka Producer using https://kafka-python.readthedocs.io/ with JSON serialization.
    """

    def __init__(self, bootstrap_servers: list, schema: dict) -> None:
        """
        Create the Producer instance.
        """
        super(KafkaProducer, self).__init__(
            bootstrap_servers=bootstrap_servers, schema=schema
        )
        self.producer = k_KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def send(self, topic: str, key: str, value: dict) -> None:
        """
        Send the specified Value to a Kafka Topic.
        """
        self.producer.send(topic, key=key, value=value)
        self.producer.flush()
