"""Kafka producer — multi-pass CDC simulation cho Yelp users (Pass 1/2/3)."""

import json
import random
import time

from . import config


def load_users(path: str = None, max_rows: int = None) -> list:
    """Đọc user.json, chỉ giữ SCD_FIELDS, trả về list of dicts."""
    path = path or config.USER_JSON_PATH
    users = []
    with open(path, "r") as f:
        for i, line in enumerate(f):
            if max_rows and i >= max_rows:
                break
            row = json.loads(line)
            users.append({k: row[k] for k in config.SCD_FIELDS if k in row})
    return users


def send_batch(producer, records: list, topic: str, delay: float = 0.0, label: str = "", batch_size: int = 500) -> int:
    """Gửi list records lên Kafka, flush mỗi batch_size record, in progress + rate."""
    total = len(records)
    sent = 0
    errors = 0
    start = time.time()

    for record in records:
        try:
            producer.send(topic, value=record)
            sent += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  ⚠️  Kafka error: {e}")

        if sent % batch_size == 0:
            producer.flush()
            elapsed = time.time() - start
            rate = sent / elapsed if elapsed > 0 else 0
            print(f"  [{label}] {sent:>8,} / {total:,}  ({rate:,.0f} rec/s)")

        if delay > 0:
            time.sleep(delay)

    producer.flush()
    elapsed = time.time() - start
    rate = total / elapsed if elapsed > 0 else 0
    print(f"  [{label}] ✅ Xong {sent:,} records  |  {elapsed:.1f}s  |  {rate:,.0f} rec/s  |  errors={errors}")
    return sent


def simulate_update(user: dict, review_delta=(1, 10), fans_delta=(0, 3), useful_delta=(0, 5), stars_jitter: float = 0.1) -> dict:
    """Pass 2: review_count/fans/useful tăng nhẹ, average_stars drift ±stars_jitter."""
    updated = user.copy()
    updated["review_count"] = user.get("review_count", 0) + random.randint(*review_delta)
    updated["fans"] = user.get("fans", 0) + random.randint(*fans_delta)
    updated["useful"] = user.get("useful", 0) + random.randint(*useful_delta)
    delta = round(random.uniform(-stars_jitter, stars_jitter), 2)
    updated["average_stars"] = round(min(5.0, max(1.0, user.get("average_stars", 3.5) + delta)), 2)
    return updated


def simulate_update_v2(user: dict, review_delta=(5, 20), fans_delta=(1, 5), cool_delta=(0, 3), stars_jitter: float = 0.15) -> dict:
    """Pass 3: update lần 2 trên user đã update ở Pass 2 — tạo version 3 trong SCD2."""
    updated = user.copy()
    updated["review_count"] = user.get("review_count", 0) + random.randint(*review_delta)
    updated["fans"] = user.get("fans", 0) + random.randint(*fans_delta)
    updated["cool"] = user.get("cool", 0) + random.randint(*cool_delta)
    delta = random.uniform(-stars_jitter, stars_jitter)
    updated["average_stars"] = round(min(5.0, max(1.0, user.get("average_stars", 3.5) + delta)), 2)
    return updated


def run_pass1(producer, all_users: list, topic: str = None, delay: float = 0.0) -> int:
    """Pass 1 — full load toàn bộ users (initial snapshot, Bronze nhận hết / Silver INSERT all)."""
    topic = topic or config.KAFKA_TOPIC_YELP_USERS
    print(f"PASS 1 — FULL LOAD ({len(all_users):,} users)")
    return send_batch(producer, all_users, topic, delay=delay, label="PASS1")


def run_pass2(producer, all_users: list, topic: str = None, pct: float = 0.30, delay: float = 0.002, seed: int = 42) -> list:
    """Pass 2 — CDC update pct% users (mặc định 30%). Trả về list users đã update (base cho Pass 3)."""
    topic = topic or config.KAFKA_TOPIC_YELP_USERS
    random.seed(seed)
    n_update = int(len(all_users) * pct)
    sample = random.sample(all_users, n_update)
    updated = [simulate_update(u) for u in sample]
    print(f"PASS 2 — CDC UPDATE ({len(updated):,} users, {pct*100:.0f}%)")
    send_batch(producer, updated, topic, delay=delay, label="PASS2")
    return updated


def run_pass3(producer, all_users: list, pass2_updated: list, topic: str = None, pct: float = 0.10, delay: float = 0.002) -> list:
    """
    Pass 3 — CDC update lần 2: pct% của TOTAL all_users (không phải % của
    pass2_updated) — lấy subset từ pass2_updated để giữ continuity, tạo
    version 3 trong SCD2 cho các user này.
    """
    topic = topic or config.KAFKA_TOPIC_YELP_USERS
    n_update = int(len(all_users) * pct)
    n_update = min(n_update, len(pass2_updated))  # safety guard
    sample = random.sample(pass2_updated, n_update)
    updated = [simulate_update_v2(u) for u in sample]
    print(f"PASS 3 — CDC UPDATE LẦN 2 ({len(updated):,} users)")
    send_batch(producer, updated, topic, delay=delay, label="PASS3")
    return updated
