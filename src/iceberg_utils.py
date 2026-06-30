"""
Helpers chung để đọc Iceberg metadata tables (.snapshots, .files) và đo
query latency — dùng trong cả monitor (Bronze/Silver) và benchmark.
"""

import time


def get_latest_total_records(spark, table: str) -> int:
    """
    Đọc total-records từ snapshot mới nhất qua metadata table — KHÔNG
    dùng SELECT COUNT(*) vì sẽ bị Iceberg write lock block trong lúc
    streaming đang commit và trả về giá trị cũ (stale).
    """
    rows = spark.sql(f"""
        SELECT CAST(summary['total-records'] AS LONG) AS total
        FROM {table}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """).collect()
    return rows[0]["total"] if rows else 0


def get_snapshot_history(spark, table: str, exclude_compaction: bool = True):
    """
    Đọc toàn bộ snapshot history (committed_at, added/deleted/total records,
    operation), sort theo thời gian tăng dần.

    exclude_compaction=True sẽ loại snapshot operation='replace' (sinh ra
    bởi CALL system.rewrite_data_files) — quan trọng cho mọi tính toán
    throughput/latency dựa trên snapshot, để không bị méo số nếu benchmark
    chạy lại sau khi đã compact mà chưa pipeline_reset.
    """
    where_clause = "WHERE operation != 'replace'" if exclude_compaction else ""
    return spark.sql(f"""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            CAST(summary['added-records']   AS LONG) AS added_records,
            CAST(summary['deleted-records'] AS LONG) AS deleted_records,
            CAST(summary['total-records']   AS LONG) AS total_records
        FROM {table}.snapshots
        {where_clause}
        ORDER BY committed_at ASC
    """).collect()


def get_file_stats(spark, table: str) -> dict:
    """Số lượng và size của data files trong 1 Iceberg table (Metric 3 — compaction)."""
    row = spark.sql(f"""
        SELECT
            COUNT(*)                 AS file_count,
            SUM(file_size_in_bytes)  AS total_bytes,
            AVG(file_size_in_bytes)  AS avg_bytes,
            MIN(file_size_in_bytes)  AS min_bytes,
            MAX(file_size_in_bytes)  AS max_bytes,
            SUM(record_count)        AS total_records
        FROM {table}.files
    """).collect()[0]
    return row.asDict()


def measure_query_time(spark, query: str, label: str = "", n_runs: int = 3, verbose: bool = True) -> float:
    """Chạy query n lần, trả về median latency (ms). In progress nếu verbose=True."""
    times = []
    for _ in range(n_runs):
        t0 = time.time()
        spark.sql(query).collect()
        times.append((time.time() - t0) * 1000)
    times.sort()
    median_ms = times[len(times) // 2]
    if verbose:
        print(f"  {label}: {median_ms:.0f} ms (median of {n_runs} runs)")
    return median_ms


def warm_up(spark, tables: list, sleep_sec: float = 3.0) -> None:
    """
    Flush cache bằng cách chạy COUNT(*) trên các table rồi sleep. Dùng
    TRƯỚC MỌI lần đo query latency (cả trước và sau compaction) để đảm
    bảo so sánh công bằng — nếu chỉ warm-up 1 bên, % speedup đo được sẽ
    lẫn hiệu ứng cache warm-up chứ không phải thuần hiệu ứng compaction.
    """
    for t in tables:
        spark.sql(f"SELECT COUNT(*) FROM {t}").collect()
    time.sleep(sleep_sec)
