"""MinIO helpers."""

from __future__ import annotations

import io

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from rico_dag.config import settings


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def object_exists(key: str) -> bool:
    client = get_s3_client()
    try:
        client.head_object(Bucket=settings.minio_bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise


def put_if_missing(*, key: str, payload: bytes, content_type: str) -> None:
    if object_exists(key):
        return
    client = get_s3_client()
    client.upload_fileobj(
        io.BytesIO(payload),
        settings.minio_bucket,
        key,
        ExtraArgs={"ContentType": content_type},
    )


def get_bytes(key: str) -> bytes:
    client = get_s3_client()
    response = client.get_object(Bucket=settings.minio_bucket, Key=key)
    return response["Body"].read()
