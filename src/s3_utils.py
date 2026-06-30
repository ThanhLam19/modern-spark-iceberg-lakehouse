"""Helpers cho MinIO (S3-compatible storage) qua boto3."""

import boto3
from botocore.client import Config

from . import config


def get_s3_client():
    """Tạo boto3 S3 client đã cấu hình sẵn endpoint/credentials MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(s3, bucket: str = config.MINIO_BUCKET) -> bool:
    """Tạo bucket nếu chưa tồn tại. Trả về True nếu vừa tạo mới."""
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket not in buckets:
        s3.create_bucket(Bucket=bucket)
        return True
    return False


def delete_s3_prefix(s3, bucket: str, prefix: str) -> int:
    """Xóa toàn bộ objects có prefix trong bucket. Trả về số objects đã xóa."""
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
            deleted += len(objects)
    return deleted


def count_objects(s3, bucket: str, prefix: str) -> int:
    """Đếm nhanh số objects có prefix (1 page, dùng cho verification)."""
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return result.get("KeyCount", 0)
