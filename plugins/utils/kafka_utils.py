from kafka import KafkaProducer
import json
import logging
import os


class Kafka_producer_utils:
    """
        Kafka util for airflow
    """

    def __init__(self):
        self._producer = None

    def _get_producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=50,
                batch_size=16384,
                buffer_memory=33554432,
                request_timeout_ms=30000,
                max_in_flight_requests_per_connection=5,
            )
        return self._producer


    def send_to_kafka(self, topic :str, key:str, message):
        """
            let Kafka to send message to target
            param
                target : topic target
                key : key value of message
                message : message
        """

        producer = self._get_producer()

        try:
            future = producer.send(
                topic=topic,
                key=key,
                value=message
            )
            record_metadata = future.get(timeout=5)

            logging.info(
                f"Kafka message sent | "
                f"topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )

        except Exception as e:
            logging.error(f"Kafka send failed: {e}", exc_info=True)
            raise


    def close_kafka(self):
        """
            Close conection to kafka when Airflow task ends
        """
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logging.info("Kafka producer closed")