"""MinIO / S3-compatible object storage adapter.

Uses plain ``boto3`` (sync) inside an executor to avoid pulling in
``aioboto3``. The bucket is created on first-use if missing (path-style
addressing + signature v4 so MinIO is happy).
"""
from __future__ import annotations

import asyncio
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from eligibility_common.logging import get_logger

log = get_logger(__name__)


def _build_client(
    *, endpoint: str, user: str, password: str, region: str = "us-east-1"
) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=user,
        aws_secret_access_key=password,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


class ObjectStorage:
    """Thin async wrapper around a boto3 S3 client."""

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        user: str = "minio",
        password: str = "minio12345",
    ) -> None:
        self._endpoint = endpoint
        self._bucket = bucket
        self._user = user
        self._password = password
        self._client: Any | None = None

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = _build_client(
                endpoint=self._endpoint, user=self._user, password=self._password
            )
        return self._client

    async def ensure_bucket(self) -> None:
        """Create the target bucket if it does not yet exist. Idempotent."""

        def _do() -> None:
            client = self._get_client()
            try:
                client.head_bucket(Bucket=self._bucket)
                return
            except ClientError as e:
                # 404 / NoSuchBucket → create.
                status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                code = e.response.get("Error", {}).get("Code")
                if status not in (404,) and code not in ("404", "NoSuchBucket", "NotFound"):
                    raise
            try:
                client.create_bucket(Bucket=self._bucket)
                log.info("storage.bucket.created", bucket=self._bucket)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    return
                raise

        await asyncio.to_thread(_do)

    async def download(self, object_key: str) -> bytes:
        """Return the full object body as bytes (entire file in memory)."""

        def _do() -> bytes:
            client = self._get_client()
            resp = client.get_object(Bucket=self._bucket, Key=object_key)
            body = resp["Body"]
            try:
                return body.read()
            finally:
                try:
                    body.close()
                except Exception:
                    pass

        return await asyncio.to_thread(_do)
