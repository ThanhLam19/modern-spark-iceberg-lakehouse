"""
Factory tạo SparkSession đã cấu hình sẵn Nessie catalog + S3A (MinIO).

Toàn bộ notebook (Bronze / Silver / Gold / Benchmark / Reset) trước đây
copy-paste cùng ~25 dòng `.config(...)` — giờ chỉ cần gọi
`get_spark_session("AppName")`.
"""

from pyspark.sql import SparkSession

from . import config


def get_spark_session(app_name: str, stop_existing: bool = False) -> SparkSession:
    """
    Tạo (hoặc lấy lại) SparkSession với Nessie catalog + S3A config đầy đủ.

    Args:
        app_name: tên app hiển thị trên Spark UI (vd: "Yelp_Bronze_Users").
        stop_existing: nếu True, stop SparkSession hiện tại (nếu có) trước
            khi tạo mới. Hữu ích khi muốn đổi appName giữa các lần chạy
            trong cùng kernel.

    Returns:
        SparkSession đã sẵn sàng. Bao gồm fix quan trọng: set lại S3A
        credentials trực tiếp vào hadoopConfiguration() sau getOrCreate()
        — builder.config("spark.hadoop.fs.s3a.*") không đủ khi notebook
        này reuse JVM của 1 SparkSession khác đã chạy trước trong cùng
        kernel (vd: benchmark notebook attach vào JVM của Bronze/Silver).
    """
    if stop_existing:
        try:
            SparkSession.builder.getOrCreate().stop()
        except Exception:
            pass

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", config.NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", config.NESSIE_REF)
        .config("spark.sql.catalog.nessie.warehouse", config.WAREHOUSE_PATH)
        .config("spark.sql.catalog.nessie.s3.endpoint", config.MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", config.MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", config.MINIO_SECRET_KEY)
        .config("spark.sql.defaultCatalog", "nessie")
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", config.MINIO_ENDPOINT)
    hc.set("fs.s3a.access.key", config.MINIO_ACCESS_KEY)
    hc.set("fs.s3a.secret.key", config.MINIO_SECRET_KEY)
    hc.set("fs.s3a.path.style.access", "true")
    hc.set("fs.s3a.connection.ssl.enabled", "false")
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_active_streams(spark: SparkSession) -> list:
    """Stop toàn bộ Structured Streaming queries đang active. Trả về tên các stream đã stop."""
    stopped = []
    for q in spark.streams.active:
        stopped.append(q.name)
        q.stop()
    return stopped
