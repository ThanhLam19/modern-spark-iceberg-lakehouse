"""
Cấu hình tập trung cho Modern Data Lakehouse pipeline.

Tất cả constants liên quan đến Nessie / MinIO / Kafka / đường dẫn data nằm
ở đây để mọi module và notebook khác import dùng chung — tránh lặp lại
config rải rác trong từng notebook (đây là nguồn của bug #6/#7 trong nhật
ký debug: thiếu S3A config đồng bộ giữa các notebook).
"""

# ── Nessie catalog ──────────────────────────────────────────────────
NESSIE_URI = "http://nessie:19120/api/v1"
NESSIE_REF = "main"
WAREHOUSE_PATH = "s3a://warehouse/"

# ── MinIO (S3-compatible storage) ───────────────────────────────────
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "warehouse"

# ── Kafka ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_YELP_USERS = "raw_yelp_users"

# ── Iceberg tables (Nessie catalog) ─────────────────────────────────
TABLE_BRONZE_USERS = "nessie.bronze.yelp_users"
TABLE_SILVER_USERS_SCD2 = "nessie.silver.yelp_users_scd2"
TABLE_GOLD_BUSINESSES = "nessie.gold.businesses_flat"

NAMESPACE_BRONZE = "nessie.bronze"
NAMESPACE_SILVER = "nessie.silver"
NAMESPACE_GOLD = "nessie.gold"

# ── Checkpoints (Spark Structured Streaming) ────────────────────────
CHECKPOINT_BRONZE = f"{WAREHOUSE_PATH}checkpoints/bronze_yelp_users"
CHECKPOINT_SILVER = f"{WAREHOUSE_PATH}checkpoints/silver_yelp_users_scd2"

# ── Data file paths (mounted trong container JupyterLab) ───────────
DATA_DIR = "/home/iceberg/data/yelp"
USER_JSON_PATH = f"{DATA_DIR}/yelp_academic_dataset_user.json"
BUSINESS_JSON_PATH = f"{DATA_DIR}/yelp_academic_dataset_business.json"

# ── Schema: field giữ lại từ user.json cho SCD2 (phải khớp Bronze schema) ──
SCD_FIELDS = {
    "user_id", "name", "review_count", "yelping_since",
    "useful", "funny", "cool", "fans", "average_stars", "elite",
}
