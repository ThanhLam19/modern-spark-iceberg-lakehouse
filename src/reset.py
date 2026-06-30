"""Pipeline reset — xóa sạch Iceberg tables, MinIO data/checkpoints, Kafka topic."""

from . import config
from .kafka_utils import reset_topic
from .s3_utils import count_objects, delete_s3_prefix

# Olist tables từ phase seminar trước — dọn luôn nếu còn sót trong catalog
EXTRA_TABLES_LEGACY = [
    "nessie.silver.customers_scd2",
    "nessie.bronze.customers",
]

EXTRA_CHECKPOINTS_LEGACY = [
    "checkpoints/bronze_customers",
    "checkpoints/silver_customers_scd2",
]


def stop_all_streams(spark) -> list:
    """Stop toàn bộ streaming queries đang active. Trả về tên các stream đã stop."""
    stopped = []
    for q in spark.streams.active:
        stopped.append(q.name)
        q.stop()
    return stopped


def drop_all_tables(spark, include_legacy: bool = True) -> None:
    """Drop Bronze/Silver/Gold tables + namespaces khỏi Nessie catalog."""
    tables = [
        config.TABLE_GOLD_BUSINESSES,
        config.TABLE_SILVER_USERS_SCD2,
        config.TABLE_BRONZE_USERS,
    ]
    if include_legacy:
        tables += EXTRA_TABLES_LEGACY

    for t in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
            print(f"  🗑  Dropped : {t}")
        except Exception as e:
            print(f"  Skip       : {t} ({e})")

    for ns in [config.NAMESPACE_GOLD, config.NAMESPACE_SILVER, config.NAMESPACE_BRONZE]:
        try:
            spark.sql(f"DROP NAMESPACE IF EXISTS {ns}")
            print(f"  🗑  Namespace dropped: {ns}")
        except Exception as e:
            print(f"  Skip namespace: {ns} ({e})")


def clean_minio_data(s3) -> None:
    """
    DROP TABLE chỉ xóa catalog entry, KHÔNG xóa .parquet/.avro/.json trên
    S3 — phải xóa thủ công để MinIO thực sự sạch.
    """
    for prefix in ["bronze/", "silver/", "gold/"]:
        n = delete_s3_prefix(s3, config.MINIO_BUCKET, prefix)
        if n > 0:
            print(f"  🗑  Deleted: {config.MINIO_BUCKET}/{prefix} ({n} objects)")
        else:
            print(f"  Skip (empty): {config.MINIO_BUCKET}/{prefix}")


def clean_checkpoints(s3, include_legacy: bool = True) -> None:
    """Xóa Spark Streaming checkpoints trên MinIO."""
    checkpoints = ["checkpoints/bronze_yelp_users", "checkpoints/silver_yelp_users_scd2"]
    if include_legacy:
        checkpoints += EXTRA_CHECKPOINTS_LEGACY

    for prefix in checkpoints:
        n = delete_s3_prefix(s3, config.MINIO_BUCKET, prefix)
        if n > 0:
            print(f"  🗑  Deleted: {config.MINIO_BUCKET}/{prefix} ({n} objects)")
        else:
            print(f"  Skip (empty): {config.MINIO_BUCKET}/{prefix}")


def verify_clean(spark, s3) -> bool:
    """Check Nessie catalog, MinIO data/checkpoints, và active streams. Trả về True nếu sạch hoàn toàn."""
    all_ok = True

    for ns in [config.NAMESPACE_BRONZE, config.NAMESPACE_SILVER, config.NAMESPACE_GOLD]:
        try:
            remaining = spark.sql(f"SHOW TABLES IN {ns}").count()
            if remaining == 0:
                print(f"  ✅ Catalog {ns}: sạch")
            else:
                print(f"  ⚠️  Catalog {ns}: còn {remaining} table chưa bị drop")
                all_ok = False
        except Exception:
            # Namespace không tồn tại = đã drop hoàn toàn = OK
            print(f"  ✅ Catalog {ns}: namespace không tồn tại (đã xóa hoàn toàn)")

    for prefix in ["bronze/", "silver/", "gold/"]:
        count = count_objects(s3, config.MINIO_BUCKET, prefix)
        if count == 0:
            print(f"  ✅ MinIO {config.MINIO_BUCKET}/{prefix}: sạch")
        else:
            print(f"  ⚠️  MinIO {config.MINIO_BUCKET}/{prefix}: còn objects")
            all_ok = False

    if count_objects(s3, config.MINIO_BUCKET, "checkpoints/") == 0:
        print("  ✅ Checkpoints: sạch")
    else:
        print("  ⚠️  Còn checkpoint objects")
        all_ok = False

    active_streams = len(spark.streams.active)
    if active_streams == 0:
        print("  ✅ Streams: không có stream nào đang chạy")
    else:
        print(f"  ⚠️  Còn {active_streams} streams đang chạy")
        all_ok = False

    return all_ok


def full_reset(spark, s3, include_legacy: bool = True) -> bool:
    """Chạy toàn bộ 6 bước reset theo thứ tự trong 1 lần gọi. Trả về True nếu verify sạch hoàn toàn."""
    print("BƯỚC 1: Stop Streaming Queries")
    stopped = stop_all_streams(spark)
    if not stopped:
        print("  Không có stream nào đang chạy")
    else:
        for name in stopped:
            print(f"  ⏹  Stopped: {name}")

    print("\nBƯỚC 2: Drop Iceberg Tables (Nessie catalog)")
    drop_all_tables(spark, include_legacy=include_legacy)

    print("\nBƯỚC 3: Xóa data/metadata files trên MinIO")
    clean_minio_data(s3)

    print("\nBƯỚC 4: Xóa Checkpoints trên MinIO")
    clean_checkpoints(s3, include_legacy=include_legacy)

    print("\nBƯỚC 5: Reset Kafka Topic")
    reset_topic(config.KAFKA_TOPIC_YELP_USERS)

    print("\nBƯỚC 6: Verification")
    return verify_clean(spark, s3)
