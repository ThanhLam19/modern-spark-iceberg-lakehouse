"""Helpers cho Kafka — admin operations và producer factory."""

import json
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from . import config


def get_kafka_admin_client(client_id: str = "lakehouse_admin") -> KafkaAdminClient:
    """KafkaAdminClient thuần Python — vì kafka-topics.sh không chạy được
    từ trong container JupyterLab (không có Kafka CLI cài sẵn)."""
    return KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        client_id=client_id,
    )


def reset_topic(topic: str, num_partitions: int = 3, replication_factor: int = 1) -> None:
    """Xóa topic (nếu tồn tại) rồi tạo lại mới, sạch hoàn toàn."""
    admin = get_kafka_admin_client("pipeline_reset")
    try:
        existing = admin.list_topics()
        if topic in existing:
            admin.delete_topics([topic])
            print(f"  🗑  Deleted topic: {topic}")
            time.sleep(5)  # chờ Kafka xử lý xong deletion
        else:
            print(f"  Skip: topic '{topic}' chưa tồn tại")

        new_topic = NewTopic(
            name=topic, num_partitions=num_partitions, replication_factor=replication_factor
        )
        admin.create_topics([new_topic])
        print(f"  ✅ Created topic: {topic} ({num_partitions} partitions)")
    finally:
        admin.close()


def get_producer() -> KafkaProducer:
    """Producer đã tune throughput (batch_size/linger/compression) cho multi-pass CDC load."""
    return KafkaProducer(
        bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        batch_size=65536,
        linger_ms=10,
        compression_type="gzip",
        acks=1,
    )
