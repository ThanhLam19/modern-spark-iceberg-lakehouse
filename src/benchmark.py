"""System benchmark v2.1 — đo 4 metric chính của pipeline sau khi đã chạy đủ
Bronze + Silver (streaming) + Gold (batch).

Metric 1: End-to-end ingestion latency  — Bronze snapshot history (batch interval)
Metric 2: SCD2 MERGE throughput          — Silver snapshot history (added/deleted records/s)
Metric 3: Compaction impact              — số file + query latency trước/sau REWRITE DATA FILES
Metric 4: Gold query latency             — 5 analytics query trên Gold layer

v2 fix: Metric 1 & 2 không dùng probe/polling hay MERGE trực tiếp — đọc snapshot
metadata, hoạt động cả khi benchmark đang reuse SparkSession của Bronze/Silver.

v2.1 fix (sau review):
- Metric 2: loại snapshot operation='replace' (sinh ra do compaction ở Metric 3)
  khỏi snapshot history, để throughput không bị méo nếu benchmark chạy lại lần 2
  mà không pipeline_reset trước.
- Metric 3: warm-up ĐỐI XỨNG cho cả lần đo "trước" và "sau" compaction, tránh
  % speedup bị lẫn hiệu ứng cache warm-up.
"""

import json
import time

# ── Query constants dùng chung cho Metric 3 & 4 ─────────────────────────

BENCH_QUERY_BRONZE = """
    SELECT
        COUNT(*) as cnt,
        AVG(review_count) as avg_reviews,
        AVG(fans) as avg_fans
    FROM nessie.bronze.yelp_users
"""

BENCH_QUERY_SILVER = """
    SELECT
        is_current,
        COUNT(*) as cnt,
        AVG(review_count) as avg_reviews,
        AVG(fans) as avg_fans
    FROM nessie.silver.yelp_users_scd2
    GROUP BY is_current
"""

BENCH_QUERY_GOLD = """
    SELECT
        state,
        COUNT(*) as total,
        ROUND(AVG(stars), 2) as avg_stars,
        SUM(CAST(attr_outdoor_seating AS INT)) as outdoor_count
    FROM nessie.gold.businesses_flat
    WHERE is_open = 1
    GROUP BY state
    ORDER BY total DESC
"""

GOLD_ANALYTICS_QUERIES = {
    "Q1_count_scan": "SELECT COUNT(*) FROM nessie.gold.businesses_flat",
    "Q2_group_by_state": """
        SELECT state, COUNT(*) as cnt, ROUND(AVG(stars),2) as avg_stars
        FROM nessie.gold.businesses_flat
        GROUP BY state ORDER BY cnt DESC
    """,
    "Q3_filter_multi_attr": """
        SELECT name, city, stars, review_count
        FROM nessie.gold.businesses_flat
        WHERE attr_outdoor_seating = true
          AND attr_restaurants_delivery = true
          AND is_open = 1
        ORDER BY stars DESC
        LIMIT 20
    """,
    "Q4_category_explode": """
        SELECT cat, COUNT(*) as cnt
        FROM nessie.gold.businesses_flat
        LATERAL VIEW explode(categories) tmp AS cat
        GROUP BY cat
        ORDER BY cnt DESC
        LIMIT 20
    """,
    "Q5_scd2_history_join": """
        SELECT
            u.user_id,
            COUNT(*) as version_count,
            MIN(u.review_count) as min_reviews,
            MAX(u.review_count) as max_reviews
        FROM nessie.silver.yelp_users_scd2 u
        GROUP BY u.user_id
        HAVING COUNT(*) > 1
        ORDER BY version_count DESC
        LIMIT 10
    """,
}

# Metric 3 target tables. Bronze được thêm vào vì đây là layer DUY NHẤT chắc
# chắn sinh ra nhiều small file (streaming append-only, 1 file/micro-batch).
# Silver (copy-on-write, không partition) và Gold (batch createOrReplace 1 lần)
# thường chỉ có 1 file ngay từ đầu — compaction trên 2 layer này có thể là no-op,
# code dưới đây tự phát hiện và cảnh báo trường hợp này thay vì báo % ảo.
TARGET_FILE_SIZE_BYTES = "134217728"
MIN_INPUT_FILES = 2

COMPACTION_TARGETS = [
    {
        "label": "bronze",
        "table": "nessie.bronze.yelp_users",
        "query": BENCH_QUERY_BRONZE,
        "query_label": "Bronze scan     ",
    },
    {
        "label": "silver",
        "table": "nessie.silver.yelp_users_scd2",
        "query": BENCH_QUERY_SILVER,
        "query_label": "Silver aggregate",
    },
    {
        "label": "gold",
        "table": "nessie.gold.businesses_flat",
        "query": BENCH_QUERY_GOLD,
        "query_label": "Gold aggregate  ",
    },
]


# ── Helpers dùng chung ───────────────────────────────────────────────────


def get_file_stats(spark, table_name: str):
    """Lấy số lượng và size của data files trong 1 Iceberg table."""
    return spark.sql(f"""
        SELECT
            COUNT(*)                 AS file_count,
            SUM(file_size_in_bytes)  AS total_bytes,
            AVG(file_size_in_bytes)  AS avg_bytes,
            MIN(file_size_in_bytes)  AS min_bytes,
            MAX(file_size_in_bytes)  AS max_bytes,
            SUM(record_count)        AS total_records
        FROM {table_name}.files
    """).collect()[0]


def measure_query_time(spark, query: str, label: str, n_runs: int = 3) -> float:
    """Chạy query n lần, trả về median latency (ms)."""
    times = []
    for _ in range(n_runs):
        t0 = time.time()
        spark.sql(query).collect()
        t1 = time.time()
        times.append((t1 - t0) * 1000)
    times.sort()
    median_ms = times[len(times) // 2]
    print(f"  {label}: {median_ms:.0f} ms (median of {n_runs} runs)")
    return median_ms


def warm_up(spark, *tables: str) -> None:
    """Flush cache executor bằng cách query lại các table trước khi đo latency.

    Phải gọi đối xứng ở cả bước đo 'trước' và 'sau' compaction (xem v2.1 fix
    ở module docstring) — nếu không, % speedup đo được sẽ lẫn hiệu ứng cache.
    """
    print("  [Warm-up] Đang flush cache...")
    for t in tables:
        spark.sql(f"SELECT COUNT(*) FROM {t}").collect()
    time.sleep(3)
    print("  [Warm-up] Done — bắt đầu đo...")


# ── Metric 1 — End-to-end Ingestion Latency ──────────────────────────────


def measure_ingestion_latency(spark) -> dict:
    """Bronze snapshot intervals -> end-to-end ingestion latency (Kafka -> Bronze)."""
    bronze_snapshots = spark.sql("""
        SELECT
            snapshot_id,
            committed_at,
            CAST(summary['added-records']   AS LONG) AS added_records,
            CAST(summary['total-records']   AS LONG) AS total_records,
            summary['spark.app.id']                  AS app_id
        FROM nessie.bronze.yelp_users.snapshots
        ORDER BY committed_at ASC
    """).collect()

    print(f"  Tổng số snapshots Bronze : {len(bronze_snapshots)}")
    print()

    result = {"bronze_total_snapshots": len(bronze_snapshots)}

    intervals = []
    for i in range(1, len(bronze_snapshots)):
        prev = bronze_snapshots[i - 1]
        curr = bronze_snapshots[i]
        delta_sec = (curr["committed_at"] - prev["committed_at"]).total_seconds()
        intervals.append(delta_sec)

    if not intervals:
        print("  ⚠️  Chỉ có 1 snapshot — cần ít nhất 2 snapshot để tính interval")
        result["ingestion_latency_sec"] = None
        return result

    avg_interval = sum(intervals) / len(intervals)
    min_interval = min(intervals)
    max_interval = max(intervals)
    # Loại bỏ outlier (lần đầu chạy thường chậm hơn do cold start)
    stable_intervals = sorted(intervals)[1:-1] if len(intervals) > 4 else intervals
    median_interval = sorted(stable_intervals)[len(stable_intervals) // 2]

    print(f"  Batch interval (avg)     : {avg_interval:.1f}s")
    print(f"  Batch interval (median)  : {median_interval:.1f}s  ← dùng giá trị này cho luận văn")
    print(f"  Batch interval (min)     : {min_interval:.1f}s")
    print(f"  Batch interval (max)     : {max_interval:.1f}s")
    print()
    print("  Trigger config           : 15s")
    print(f"  → End-to-end latency ≈ median batch interval = {median_interval:.1f}s")
    print("    (record vào Kafka → commit vào Iceberg Bronze)")

    print()
    print("  5 snapshots gần nhất:")
    for s in bronze_snapshots[-5:]:
        print(f"    {s['committed_at'].strftime('%H:%M:%S')} | "
              f"+{s['added_records']:>7,} records | "
              f"total: {s['total_records']:>10,}")

    result.update({
        "ingestion_latency_sec": round(median_interval, 1),
        "ingestion_latency_avg_sec": round(avg_interval, 1),
        "ingestion_latency_min_sec": round(min_interval, 1),
        "ingestion_latency_max_sec": round(max_interval, 1),
    })
    return result


# ── Metric 2 — SCD2 MERGE Throughput ─────────────────────────────────────


def measure_silver_throughput(spark) -> dict:
    """Silver snapshot history -> MERGE throughput + latency per micro-batch.

    v2.1 fix: loại snapshot operation='replace' (compaction) khỏi tính toán —
    xem module docstring.
    """
    result = {}

    # 2A — Silver table stats
    silver_stats = spark.sql("""
        SELECT
            COUNT(*)                                              AS total_records,
            COUNT(DISTINCT user_id)                               AS unique_users,
            SUM(CASE WHEN is_current = false THEN 1 ELSE 0 END)  AS expired_records,
            SUM(CASE WHEN is_current = true  THEN 1 ELSE 0 END)  AS active_records,
            MIN(effective_time)                                   AS first_record_time,
            MAX(effective_time)                                   AS last_record_time
        FROM nessie.silver.yelp_users_scd2
    """).collect()[0]

    total_records   = silver_stats["total_records"]
    unique_users    = silver_stats["unique_users"]
    expired_records = silver_stats["expired_records"]
    active_records  = silver_stats["active_records"]

    print(f"  Total records in Silver  : {total_records:,}")
    print(f"  Unique users (SCD keys)  : {unique_users:,}")
    print(f"  Active records (current) : {active_records:,}")
    print(f"  Expired (SCD2 history)   : {expired_records:,} "
          f"({expired_records/total_records*100:.1f}% of total)")
    print()

    # 2B — Throughput từ snapshot history (loại snapshot 'replace')
    silver_snap = spark.sql("""
        SELECT
            MIN(committed_at)                                      AS first_commit,
            MAX(committed_at)                                      AS last_commit,
            COUNT(*)                                               AS total_snapshots,
            SUM(CAST(summary['added-records']   AS LONG))         AS total_added,
            SUM(CAST(summary['deleted-records'] AS LONG))         AS total_deleted,
            SUM(COALESCE(CAST(summary['added-records'] AS LONG), 0)
              + COALESCE(CAST(summary['deleted-records'] AS LONG), 0)) AS total_ops
        FROM nessie.silver.yelp_users_scd2.snapshots
        WHERE operation != 'replace'
    """).collect()[0]

    excluded_snap = spark.sql("""
        SELECT COUNT(*) AS cnt
        FROM nessie.silver.yelp_users_scd2.snapshots
        WHERE operation = 'replace'
    """).collect()[0]["cnt"]

    if excluded_snap > 0:
        print(f"  ⚠️  Đã loại {excluded_snap} snapshot 'replace' (compaction cũ) "
              "khỏi tính toán throughput")
        print()

    first_commit    = silver_snap["first_commit"]
    last_commit     = silver_snap["last_commit"]
    total_snapshots = silver_snap["total_snapshots"]
    total_added     = silver_snap["total_added"] or 0
    total_deleted   = silver_snap["total_deleted"] or 0
    total_ops       = silver_snap["total_ops"] or 0

    if first_commit and last_commit:
        duration_sec = (last_commit - first_commit).total_seconds()
        throughput_rps = total_records / duration_sec if duration_sec > 0 else 0
        ops_rps = total_ops / duration_sec if duration_sec > 0 else 0
    else:
        duration_sec = None
        throughput_rps = None
        ops_rps = None

    print("  Silver snapshot history (đã loại snapshot compaction):")
    print(f"    Total snapshots          : {total_snapshots}  "
          "(2 per micro-batch = MERGE step1 + step2)")
    print(f"    Total added ops          : {total_added:,}")
    print(f"    Total deleted ops        : {total_deleted:,}")
    if duration_sec:
        print(f"    Pipeline duration        : {duration_sec:.0f}s ({duration_sec/60:.1f} phút)")
        print(f"    Throughput (records/s)   : {throughput_rps:,.1f}  "
              "← dùng giá trị này cho luận văn")
        print(f"    MERGE ops/s (add+del)    : {ops_rps:,.1f}")
    else:
        print("    Pipeline duration        : N/A")

    # 2C — MERGE latency từ snapshot interval (pair 2 snapshot = 1 micro-batch)
    print()
    print("  MERGE latency per micro-batch (từ Silver snapshot intervals):")
    silver_snaps_list = spark.sql("""
        SELECT committed_at,
               CAST(summary['added-records']   AS LONG) AS added,
               CAST(summary['deleted-records'] AS LONG) AS deleted
        FROM nessie.silver.yelp_users_scd2.snapshots
        WHERE operation != 'replace'
        ORDER BY committed_at ASC
    """).collect()

    merge_latencies = []
    for i in range(0, len(silver_snaps_list) - 1, 2):
        t_start = silver_snaps_list[i]["committed_at"]
        t_end = silver_snaps_list[i + 1]["committed_at"]
        delta_ms = (t_end - t_start).total_seconds() * 1000
        merge_latencies.append(delta_ms)

    if merge_latencies:
        merge_latencies_sorted = sorted(merge_latencies)
        median_merge_ms = merge_latencies_sorted[len(merge_latencies_sorted) // 2]
        avg_merge_ms = sum(merge_latencies) / len(merge_latencies)
        print(f"    Số micro-batch đo được   : {len(merge_latencies)}")
        print(f"    MERGE latency (avg)      : {avg_merge_ms:.0f} ms")
        print(f"    MERGE latency (median)   : {median_merge_ms:.0f} ms  "
              "← dùng giá trị này cho luận văn")
        result["merge_latency_ms"] = round(median_merge_ms)
    else:
        print("    Không đủ snapshot để tính MERGE latency")
        result["merge_latency_ms"] = None

    result["silver_total_records"] = int(total_records)
    result["silver_expired_records"] = int(expired_records)
    result["silver_active_records"] = int(active_records)
    result["silver_throughput_rps"] = round(throughput_rps, 1) if throughput_rps else None
    result["silver_pipeline_duration_sec"] = round(duration_sec) if duration_sec else None

    return result


# ── Metric 3 — Compaction Impact ─────────────────────────────────────────


def measure_files_before_compaction(spark, targets: list = None):
    """Metric 3A: file stats + query latency TRƯỚC compaction, cho mọi target
    trong `targets` (mặc định COMPACTION_TARGETS — Bronze/Silver/Gold).

    Trả về (raw, result):
    - raw: dict thô {label: {"stats": ..., "qtime_before": ...}, "targets": targets}
      cần truyền lại cho measure_files_after_compaction().
    - result: dict gọn để update() trực tiếp vào `results` của notebook.
    """
    targets = targets or COMPACTION_TARGETS
    raw = {"targets": targets}
    result = {}

    for t in targets:
        label, table = t["label"], t["table"]
        stats = get_file_stats(spark, table)
        print(f"  [{label.capitalize()}] Files: {stats['file_count']:,} | "
              f"Total: {stats['total_bytes']/1024/1024:.1f} MB | "
              f"Avg: {stats['avg_bytes']/1024:.1f} KB | "
              f"Min: {stats['min_bytes']/1024:.1f} KB")
        if stats["file_count"] < MIN_INPUT_FILES:
            print(f"  ⚠️  [{label.capitalize()}] chỉ có {stats['file_count']} file "
                  f"— rewrite_data_files (min-input-files={MIN_INPUT_FILES}) sẽ là no-op")
        raw[label] = {"stats": stats}
        result[f"{label}_files_before"] = int(stats["file_count"])

    # v2.1 fix: warm-up TRƯỚC khi đo "before" — đối xứng với warm-up "after".
    print()
    warm_up(spark, *[t["table"] for t in targets])

    print()
    print("  Query time trước compaction:")
    for t in targets:
        label = t["label"]
        qtime = measure_query_time(spark, t["query"], t["query_label"])
        raw[label]["qtime_before"] = qtime
        result[f"{label}_query_before_ms"] = round(qtime)

    return raw, result


def run_compaction(spark, targets: list = None) -> dict:
    """Metric 3B: chạy Iceberg REWRITE DATA FILES cho mọi target (Bronze/Silver/Gold)."""
    targets = targets or COMPACTION_TARGETS
    result = {}

    for t in targets:
        label, table = t["label"], t["table"]
        print(f"  [{label.capitalize()}] Đang compact...")
        t0 = time.time()
        spark.sql(f"""
            CALL nessie.system.rewrite_data_files(
                table  => '{table}',
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                    'min-input-files', '{MIN_INPUT_FILES}'
                )
            )
        """).show()
        compact_time = time.time() - t0
        print(f"  [{label.capitalize()}] Compaction xong: {compact_time:.1f}s")
        print()
        result[f"{label}_compaction_time_sec"] = round(compact_time, 1)

    return result


def measure_files_after_compaction(spark, raw_before: dict, targets: list = None) -> dict:
    """Metric 3C: file stats + query latency SAU compaction, tính % cải thiện.

    `raw_before` là dict trả về từ measure_files_before_compaction(). Nếu số file
    không đổi (compaction no-op vì <MIN_INPUT_FILES file), `*_query_speedup_pct`
    được set None thay vì 1 số liệu gây hiểu lầm — chênh lệch query time trong
    trường hợp đó CHỈ LÀ NHIỄU cache/JIT, không phải hiệu ứng compaction.
    """
    targets = targets or raw_before.get("targets") or COMPACTION_TARGETS
    result = {}

    print()
    print("  Query time sau compaction:")
    # Warm-up: flush executor cache sau compaction trước khi đo
    # (tránh trường hợp query after chậm hơn before do cold cache)
    warm_up(spark, *[t["table"] for t in targets])

    for t in targets:
        label, table = t["label"], t["table"]
        before_stats = raw_before[label]["stats"]
        qtime_before = raw_before[label]["qtime_before"]

        after_stats = get_file_stats(spark, table)
        qtime_after = measure_query_time(spark, t["query"], t["query_label"])

        files_before = before_stats["file_count"]
        files_after = after_stats["file_count"]
        file_reduction = (1 - files_after / files_before) * 100 if files_before else 0

        print(f"  [{label.capitalize()}] Files: {files_before:,} → {files_after:,} "
              f"({file_reduction:+.0f}%) | Query: {qtime_before:.0f}ms → {qtime_after:.0f}ms")

        result[f"{label}_files_after"] = int(files_after)
        result[f"{label}_query_after_ms"] = round(qtime_after)
        result[f"{label}_file_reduction_pct"] = round(file_reduction, 1)

        if files_before == files_after:
            print(f"  ⚠️  [{label.capitalize()}] số file không đổi → compaction là no-op. "
                  "% query speedup KHÔNG được tính (sẽ chỉ là nhiễu cache/JIT, không phải "
                  "hiệu ứng compaction) — set None.")
            result[f"{label}_query_speedup_pct"] = None
            result[f"{label}_compaction_effective"] = False
        else:
            query_speedup = (1 - qtime_after / qtime_before) * 100 if qtime_before else 0
            result[f"{label}_query_speedup_pct"] = round(query_speedup, 1)
            result[f"{label}_compaction_effective"] = True

    return result


# ── Metric 4 — Gold Query Latency ────────────────────────────────────────


def measure_gold_query_latency(spark, n_runs: int = 3) -> dict:
    """5 analytics query trên Gold (+ Q5 join lịch sử SCD2 ở Silver)."""
    query_results = {}
    for name, q in GOLD_ANALYTICS_QUERIES.items():
        ms = measure_query_time(spark, q, name, n_runs=n_runs)
        query_results[name] = round(ms)

    print()
    print("  Chú thích:")
    print("  Q1: Full scan + count (đo I/O baseline)")
    print("  Q2: GROUP BY state (đo aggregation)")
    print("  Q3: Multi-column filter (đo predicate pushdown)")
    print("  Q4: LATERAL VIEW explode array (đo array processing)")
    print("  Q5: SCD2 history query — full scan Silver + GROUP BY + HAVING.")
    print("      Latency cao hơn Q1-Q4 là trade-off bình thường của SCD2 pattern:")
    print("      mỗi user_id có nhiều version row → không thể predicate pushdown.")
    print("      Đây là chi phí của việc lưu lịch sử thay đổi (history preservation).")

    return {"gold_queries_ms": query_results}


# ── Tổng hợp kết quả ──────────────────────────────────────────────────────


def print_summary(results: dict, save_path: str) -> None:
    """In bảng tổng hợp kết quả benchmark (copy thẳng vào luận văn) + lưu JSON."""
    w = 65

    def row(text):
        print(f"║  {text:<{w-2}}║")

    def sep():
        print("╠" + "═" * w + "╣")

    print()
    print("╔" + "═" * w + "╗")
    row("KẾT QUẢ BENCHMARK — MODERN DATA LAKEHOUSE")
    sep()

    row("METRIC 1: END-TO-END INGESTION LATENCY (Kafka → Bronze)")
    row(f"  Batch interval (median)  : {results.get('ingestion_latency_sec', 'N/A')} giây")
    row(f"  Batch interval (avg)     : {results.get('ingestion_latency_avg_sec', 'N/A')} giây")
    row(f"  Batch interval (min/max) : {results.get('ingestion_latency_min_sec', 'N/A')}s / "
        f"{results.get('ingestion_latency_max_sec', 'N/A')}s")
    row("  Trigger config           : 15s")
    sep()

    row("METRIC 2: SILVER SCD2 THROUGHPUT")
    _silver_total = results.get("silver_total_records", "N/A")
    _silver_active = results.get("silver_active_records", "N/A")
    _silver_expired = results.get("silver_expired_records", "N/A")
    row(f"  Total records Silver     : {_silver_total:,}" if isinstance(_silver_total, int)
        else f"  Total records Silver     : {_silver_total}")
    row(f"  Active (is_current=true) : {_silver_active:,}" if isinstance(_silver_active, int)
        else f"  Active (is_current=true) : {_silver_active}")
    row(f"  Expired (SCD2 history)   : {_silver_expired:,}" if isinstance(_silver_expired, int)
        else f"  Expired (SCD2 history)   : {_silver_expired}")
    row(f"  Pipeline duration        : {results.get('silver_pipeline_duration_sec', 'N/A')}s")
    row(f"  Avg throughput           : {results.get('silver_throughput_rps', 'N/A')} records/s")
    row(f"  MERGE latency (median)   : {results.get('merge_latency_ms', 'N/A')} ms")
    sep()

    row("METRIC 3: COMPACTION IMPACT")
    for label in ("bronze", "silver", "gold"):
        files_before = results.get(f"{label}_files_before", "?")
        files_after = results.get(f"{label}_files_after", "?")
        reduction = results.get(f"{label}_file_reduction_pct", "?")
        q_before = results.get(f"{label}_query_before_ms", "?")
        q_after = results.get(f"{label}_query_after_ms", "?")
        speedup = results.get(f"{label}_query_speedup_pct")
        speedup_str = f"{speedup}% faster" if speedup is not None else "N/A — no-op, không tính"
        row(f"  {label.capitalize():<7}: {files_before} files → {files_after} files  "
            f"(-{reduction}%)")
        row(f"  {label.capitalize()} query : {q_before}ms → {q_after}ms  ({speedup_str})")
    sep()

    row("METRIC 4: GOLD QUERY LATENCY (sau compaction)")
    if "gold_queries_ms" in results:
        for qname, ms in results["gold_queries_ms"].items():
            row(f"  {qname:<32}: {ms:>6} ms")

    print("╚" + "═" * w + "╝")

    print()
    print(f"Lưu kết quả vào file {save_path}...")
    with open(save_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"✅ Saved: {save_path}")