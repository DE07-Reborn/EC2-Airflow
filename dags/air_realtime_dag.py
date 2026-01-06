from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from utils.air_realtime_extract import extract_realtime
from utils.air_realtime_transform import transform_realtime
from utils.air_realtime_load_s3 import save_raw_to_s3
from utils.air_kafka_producer import KafkaProducerUtils
from utils.air_config import settings

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="air_quality_realtime_collect",
    start_date=datetime(2025, 1, 1),
    schedule_interval="18 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["air-quality", "realtime"],
) as dag:

    @task(task_id="extract")
    def extract():
        return extract_realtime()

    @task(task_id="save_raw_s3")
    def save_raw(raw_data: dict):
        save_raw_to_s3(raw_data)

    @task(task_id="transform_for_kafka")
    def transform(raw_data: dict):
        return transform_realtime(raw_data)

    @task(task_id="publish_kafka")
    def publish(transformed: list[dict]):
        producer = KafkaProducerUtils()
        for record in transformed:
            key = f"{record['station_code']}"
            producer.send(
                topic=settings.KAFKA_TOPIC_REALTIME,
                key=key,
                message=record,
            )
        producer.close()

    raw = extract()
    save_raw(raw)
    transformed = transform(raw)
    publish(transformed)