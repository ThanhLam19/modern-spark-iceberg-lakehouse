"""Bronze layer — Kafka → Iceberg streaming ingestion cho Yelp users (raw landing zone)."""

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from . import config

# Schema phải khớp với SCD_FIELDS dùng trong Kafka producer (xem config.py)
USER_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", LongType(), True),
        StructField("yelping_since", StringType(), True),
        StructField("useful", LongType(), True),
        StructField("funny", LongType(), True),
        StructField("cool", LongType(), True),
        StructField("fans", LongType(), True),
        StructField("average_stars", DoubleType(), True),
        StructField("elite", StringType(), True),
    ]
)


def create_bronze_table(spark) -> None:
    """Tạo namespace + table Bronze nếu chưa có (idempotent)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.NAMESPACE_BRONZE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.TABLE_BRONZE_USERS} (
            user_id       STRING,
            name          STRING,
            review_count  BIGINT,
            yelping_since STRING,
            useful        BIGINT,
            funny         BIGINT,
            cool          BIGINT,
            fans          BIGINT,
            average_stars DOUBLE,
            elite         STRING
        ) USING iceberg
    """)


def start_bronze_stream(spark, trigger_seconds: int = 15):
    """
    Đọc stream từ Kafka topic raw_yelp_users, parse JSON theo USER_SCHEMA,
    ghi append vào Bronze Iceberg table. Bronze giữ nguyên mọi record kể
    cả duplicate — raw landing zone, không dedup ở layer này.

    Trả về StreamingQuery để caller theo dõi/stop.
    """
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC_YELP_USERS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100000)
        .load()
    )

    parsed = (
        kafka_stream.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), USER_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("user_id").isNotNull())
    )

    return (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", config.CHECKPOINT_BRONZE)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .queryName("bronze_yelp_users")
        .toTable(config.TABLE_BRONZE_USERS)
    )
