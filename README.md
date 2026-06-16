# Modern Near Real-Time Data Lakehouse 🚀

![Data Engineering](https://img.shields.io/badge/Data%20Engineering-Graduation%20Thesis-blue)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.3-orange)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.8.1-00d0d4)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-black)

## 📌 Project Overview
This repository contains the prototype implementation of an end-to-end Data Lakehouse architecture designed to process the 8GB+ Yelp NDJSON dataset. The core objective is to handle near real-time data ingestion, perform SCD Type 2 updates with ACID guarantees, and flatten deeply nested structures.

> **Current Phase:** Proof of Concept (PoC) & Benchmarking.
> The core business logic and system configurations have been successfully validated using Jupyter Notebooks. *Currently in the process of refactoring these notebooks into modular `.py` scripts for Airflow/Mage orchestration.*

## 🛠️ Tech Stack
* **Compute:** PySpark (Structured Streaming & Batch)
* **Storage Format:** Apache Iceberg
* **Object Storage:** MinIO (S3-Compatible)
* **Catalog:** Nessie (RocksDB backed)
* **Message Broker:** Apache Kafka

## 📂 Notebook Guide (Pipeline Flow)
To understand the data flow, please review the Jupyter Notebooks in the following order:

1. **`data_acquisition.ipynb` & `yelp_explore.ipynb`**: Automated dataset retrieval via Kaggle CLI and initial EDA on schema complexities.
2. **`kafka_producer_yelp.ipynb`**: A custom multi-pass CDC simulator that pushes Yelp data into Kafka topics to simulate real-world streaming updates.
3. **`spark_bronze_yelp.ipynb`**: PySpark Structured Streaming job consuming Kafka topics and persisting raw JSON into Iceberg Bronze tables.
4. **`spark_silver_yelp_scd2_fixed.ipynb`**: The core logic! Uses idempotent `MERGE INTO` statements to track historical changes (review counts, fans) generating SCD Type 2 records.
5. **`spark_gold_businesses.ipynb`**: Complex data modeling job using Spark Catalyst Optimizer techniques (Array of Structs, Explode) to flatten highly nested JSON attributes for analytics.
6. **`system_benchmark_v2.ipynb`**: System evaluation tracking Ingestion Latency, MERGE Throughput, and query optimization via Iceberg Compaction (Rewrite Data Files).

## 🚀 Key Engineering Achievements
* Resolved critical **403 Access Denied (Architectural Fragmentation)** between Spark Native and MinIO by implementing Direct JVM Context Injection for Hadoop AWS configurations.
* Verified strictly ACID compliance across distributed storage using Iceberg's snapshot isolation and time-travel capabilities.
