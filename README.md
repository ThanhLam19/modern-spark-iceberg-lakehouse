# 🌊 End-to-End Modern Data Lakehouse (Spark & Iceberg)

This project establishes a robust, containerized Data Lakehouse architecture using Apache Spark for distributed computing and Apache Iceberg for table format governance (ACID, Schema Evolution).

## 💡 Engineering Highlights
 ACID Transactions & SCD Type 2 Implemented a One-Shot MERGE strategy via Spark SQL to manage Slowly Changing Dimensions (SCD Type 2) on the Iceberg table, ensuring 100% data consistency and historical tracking.
 Performance Optimization (Nested Data) Applied Denormalization by modeling order items into ARRAYSTRUCT types. This drastically reduces query latency by eliminating large-scale JOIN operations typical of traditional Star Schemas.
 Time Travel Capability Demonstrated data audit by querying historical snapshots using Iceberg's `snapshot-id` feature.
 Containerized Infrastructure Built the entire environment using Docker Compose, linking Spark (Compute) and MinIO (S3-compatible Storage) for rapid, reproducible deployment.

## 🛠 Tech Stack
 Compute Apache Spark 3.3 (PySpark)
 Table Format Apache Iceberg
 Storage MinIO (S3 Compatible Object Storage)
 Infrastructure Docker Compose

## 📁 Installation & Usage
To deploy the system, run `docker-compose up -d` in the `infrastructure` directory. Access the Jupyter Notebook at `httplocalhost8889`.
## 💾 Data Setup
The project requires the raw Olist CSV files to be placed in the MinIO storage container.

1.  **Launch MinIO:** Run `docker-compose up -d minio`.
2.  **Access MinIO Console:** Go to `http://localhost:9001` (User: `admin`, Pass: `password`).
3.  **Upload Data:** Create a bucket named `warehouse` and upload the raw Olist CSV files into the path `warehouse/raw/`.