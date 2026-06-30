"""Modern Data Lakehouse — shared pipeline modules.

Mọi notebook import từ package này thay vì copy-paste config/SparkSession
boilerplate. Cấu trúc:

    config.py          constants (Nessie/MinIO/Kafka/paths/schema)
    spark_session.py   SparkSession factory (Nessie + S3A đã cấu hình sẵn)
    s3_utils.py         boto3 helpers (MinIO)
    kafka_utils.py       Kafka admin/producer helpers
    iceberg_utils.py    snapshot history / file-stats / query-timer helpers
    bronze.py            Bronze layer (Kafka -> Iceberg streaming)
    silver.py             Silver layer (SCD Type 2 MERGE, copy-on-write)
    gold.py               Gold layer (flatten nested business.json)
    producer.py           Kafka producer — multi-pass CDC simulation
    reset.py              Pipeline teardown / reset
    benchmark.py          4-metric system benchmark
"""

__version__ = "1.0.0"
