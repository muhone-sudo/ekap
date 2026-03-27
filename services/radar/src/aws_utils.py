"""AWS helpers: S3 upload and SQS operations."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import boto3

from shared.logger import get_logger
from shared.schemas import ManifestMessage

log = get_logger(__name__)


def _s3_client():  # type: ignore[no-untyped-def]
    return boto3.client("s3")


def _sqs_client():  # type: ignore[no-untyped-def]
    return boto3.client("sqs")


def upload_to_s3(file_path: str, bucket: str, key: Optional[str] = None) -> str:
    """Upload *file_path* to S3 and return the S3 URI.

    Args:
        file_path: Local path of the file to upload.
        bucket: S3 bucket name.
        key: Optional S3 object key; defaults to the file basename.

    Returns:
        S3 URI in the form ``s3://<bucket>/<key>``.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    object_key = key or path.name
    s3 = _s3_client()

    log.info("s3_upload_start", bucket=bucket, key=object_key, size=path.stat().st_size)
    s3.upload_file(str(path), bucket, object_key)
    s3_uri = f"s3://{bucket}/{object_key}"
    log.info("s3_upload_done", s3_uri=s3_uri)
    return s3_uri


def send_manifest(manifest: ManifestMessage, queue_url: str) -> str:
    """Send *manifest* as a JSON message to SQS.

    Returns:
        SQS MessageId.
    """
    sqs = _sqs_client()
    body = manifest.model_dump_json_str()
    log.info("sqs_send", queue_url=queue_url, tender_id=manifest.tender_id)
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        MessageAttributes={
            "tender_id": {
                "DataType": "String",
                "StringValue": manifest.tender_id,
            },
            "correlation_id": {
                "DataType": "String",
                "StringValue": manifest.correlation_id or "",
            },
        },
    )
    message_id: str = response["MessageId"]
    log.info("sqs_sent", message_id=message_id)
    return message_id


def reset_sqs_visibility(queue_url: str, receipt_handle: str) -> None:
    """Reset the visibility timeout of an SQS message to 0 (NACK).

    Used by the SIGTERM handler so the message is requeued immediately.
    """
    sqs = _sqs_client()
    log.info("sqs_visibility_reset", receipt_handle=receipt_handle[:20] + "...")
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=0,
    )
