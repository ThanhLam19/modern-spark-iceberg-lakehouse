"""Silver layer — SCD Type 2 MERGE cho Yelp users (copy-on-write)."""

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from . import config


def create_silver_table(spark) -> None:
    """
    Tạo namespace + table Silver SCD2 nếu chưa có.

    copy-on-write là BẮT BUỘC ở đây — merge-on-read không tương thích với
    foreachBatch + MERGE INTO trong Spark Structured Streaming (gây crash
    PositionDeltaWrite). Đây là bug đầu tiên được tìm ra trong quá trình
    debug pipeline này.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.NAMESPACE_SILVER}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.TABLE_SILVER_USERS_SCD2} (
            user_id        STRING,
            name           STRING,
            review_count   BIGINT,
            yelping_since  STRING,
            useful         BIGINT,
            funny          BIGINT,
            cool           BIGINT,
            fans           BIGINT,
            average_stars  DOUBLE,
            elite          STRING,
            -- SCD2 metadata
            is_current     BOOLEAN,
            effective_time TIMESTAMP,
            end_time       TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'write.merge.mode' = 'copy-on-write',
            'write.update.mode' = 'copy-on-write'
        )
    """)


def make_process_batch_fn(spark):
    """
    Trả về function process_silver_scd2(micro_batch_df, batch_id) để truyền
    vào .foreachBatch(...). Đóng `spark` qua closure vì signature của
    foreachBatch cố định là (df, batch_id) — không nhận thêm tham số.

    Logic:
      BƯỚC 1 — đóng record cũ khi review_count/fans/average_stars thay đổi
               (is_current=false, end_time=now()).
      BƯỚC 2 — insert record mới: user mới HOÀN TOÀN, hoặc user vừa được
               đóng ở bước 1 (tạo version kế tiếp).
      Không thay đổi gì → bỏ qua (idempotent).
    """

    def process_silver_scd2(micro_batch_df, batch_id):
        if micro_batch_df.isEmpty():
            return

        # Dedup trong cùng micro-batch — giữ record có review_count cao
        # nhất của mỗi user_id (proxy cho "bản ghi mới nhất" trong 1 batch).
        w = Window.partitionBy("user_id").orderBy(col("review_count").desc())
        dedup_df = (
            micro_batch_df.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        dedup_df.createOrReplaceGlobalTempView("yelp_user_updates")

        spark.sql(f"""
            MERGE INTO {config.TABLE_SILVER_USERS_SCD2} AS target
            USING (
                SELECT u.*
                FROM global_temp.yelp_user_updates u
                JOIN {config.TABLE_SILVER_USERS_SCD2} t
                  ON u.user_id = t.user_id
                 AND t.is_current = true
                 AND (
                     u.review_count  != t.review_count
                  OR u.fans          != t.fans
                  OR u.average_stars != t.average_stars
                 )
            ) AS source
            ON target.user_id = source.user_id
               AND target.is_current = true
            WHEN MATCHED THEN
                UPDATE SET
                    target.is_current = false,
                    target.end_time   = current_timestamp()
        """)

        spark.sql(f"""
            MERGE INTO {config.TABLE_SILVER_USERS_SCD2} AS target
            USING (
                SELECT u.*
                FROM global_temp.yelp_user_updates u
                LEFT JOIN {config.TABLE_SILVER_USERS_SCD2} t
                  ON u.user_id = t.user_id
                 AND t.is_current = true
                WHERE t.user_id IS NULL
            ) AS source
            ON target.user_id = source.user_id
               AND target.is_current = true
            WHEN NOT MATCHED THEN
                INSERT (
                    user_id, name, review_count, yelping_since,
                    useful, funny, cool, fans, average_stars, elite,
                    is_current, effective_time, end_time
                )
                VALUES (
                    source.user_id, source.name, source.review_count,
                    source.yelping_since, source.useful, source.funny,
                    source.cool, source.fans, source.average_stars,
                    source.elite,
                    true, current_timestamp(), NULL
                )
        """)

    return process_silver_scd2


def start_silver_stream(spark, trigger_seconds: int = 20):
    """Đọc stream từ Bronze, áp dụng SCD2 MERGE qua foreachBatch. Trả về StreamingQuery."""
    process_fn = make_process_batch_fn(spark)

    bronze_stream = spark.readStream.format("iceberg").load(config.TABLE_BRONZE_USERS)

    return (
        bronze_stream.writeStream.foreachBatch(process_fn)
        .outputMode("append")
        .option("checkpointLocation", config.CHECKPOINT_SILVER)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .queryName("silver_yelp_users_scd2")
        .start()
    )


def get_scd2_stats(spark) -> dict:
    row = spark.sql(f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT user_id) AS unique_users,
            COALESCE(
                SUM(CASE WHEN is_current = false THEN 1 ELSE 0 END),
                0
            ) AS expired_records,
            COALESCE(
                SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END),
                0
            ) AS active_records
        FROM {config.TABLE_SILVER_USERS_SCD2}
    """).collect()[0]

    return row.asDict()


def get_users_with_history(spark, min_versions: int = 2, limit: int = 10):
    """Users có >= min_versions record — dùng để validate SCD2 đang hoạt động đúng."""
    return spark.sql(f"""
        SELECT
            user_id,
            COUNT(*) AS version_count,
            MIN(effective_time) AS first_seen,
            MAX(effective_time) AS last_changed
        FROM {config.TABLE_SILVER_USERS_SCD2}
        GROUP BY user_id
        HAVING COUNT(*) >= {min_versions}
        ORDER BY version_count DESC
        LIMIT {limit}
    """)
