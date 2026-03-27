"""Main worker entry point for the Radar service.

Responsibilities:
* Read a tender_id from the environment (or SQS message).
* Scrape EKAPv2 to download the tender ZIP.
* Upload ZIP to S3.
* Send a ManifestMessage to SQS.
* Handle SIGTERM by resetting SQS visibility (NACK).
* Clean up local outputs/ after upload.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import signal
import sys
import uuid
from pathlib import Path
from types import FrameType
from typing import Optional

from dotenv import load_dotenv

# Ensure shared packages are importable when running from the repo root or
# directly from the services/radar directory.
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from shared.logger import configure_logging, correlation_id_var, get_logger
from shared.schemas import ManifestMessage, ScraperConfig, TenderStatus

from .aws_utils import reset_sqs_visibility, send_manifest, upload_to_s3
from .scraper import EKAPScraper

load_dotenv()

log = get_logger(__name__)

# Set by the SQS consumer so SIGTERM can NACK the in-flight message.
_current_receipt_handle: Optional[str] = None
_queue_url: Optional[str] = None


def _sigterm_handler(signum: int, frame: Optional[FrameType]) -> None:
    """Reset SQS visibility timeout to 0 (NACK) on SIGTERM (spot termination)."""
    log.warning("sigterm_received", action="nack_sqs_message")
    if _current_receipt_handle and _queue_url:
        try:
            reset_sqs_visibility(_queue_url, _current_receipt_handle)
        except Exception as exc:  # noqa: BLE001
            log.error("sigterm_nack_failed", error=str(exc))
    sys.exit(0)


signal.signal(signal.SIGTERM, _sigterm_handler)


def _build_config() -> ScraperConfig:
    return ScraperConfig(
        headless=os.getenv("HEADLESS", "true").lower() == "true",
        jitter_min=float(os.getenv("SCRAPER_JITTER_MIN", "1.5")),
        jitter_max=float(os.getenv("SCRAPER_JITTER_MAX", "4.5")),
        block_threshold=float(os.getenv("BLOCK_THRESHOLD", "0.15")),
        s3_bucket=os.environ["S3_BUCKET_NAME"],
        sqs_queue_url=os.environ["SQS_QUEUE_URL"],
    )


async def process_tender(tender_id: str) -> None:
    """Full pipeline: scrape -> upload -> publish manifest."""
    global _current_receipt_handle, _queue_url

    config = _build_config()
    _queue_url = config.sqs_queue_url

    correlation_id = str(uuid.uuid4())
    correlation_id_var.set(correlation_id)

    log.info("tender_processing_start", tender_id=tender_id, correlation_id=correlation_id)

    scraper = EKAPScraper(config)

    output_dir = Path("outputs") / tender_id
    try:
        zip_path = await scraper.scrape_tender(tender_id, str(output_dir))
    except Exception as exc:
        log.error("scrape_failed", tender_id=tender_id, error=str(exc))
        raise

    try:
        s3_key = f"tenders/{tender_id}/{Path(zip_path).name}"
        s3_uri = upload_to_s3(zip_path, config.s3_bucket, s3_key)
    except OSError as exc:
        # Disk-full or other IO error - log and NACK
        log.error(
            "upload_failed_disk_error",
            tender_id=tender_id,
            error=str(exc),
        )
        raise

    manifest = ManifestMessage(
        tender_id=tender_id,
        s3_uri=s3_uri,
        file_size_bytes=Path(zip_path).stat().st_size,
        status=TenderStatus.UPLOADED,
        correlation_id=correlation_id,
    )
    send_manifest(manifest, config.sqs_queue_url)

    # Clean up local outputs after successful upload
    try:
        shutil.rmtree(str(output_dir), ignore_errors=True)
        log.info("outputs_cleaned", path=str(output_dir))
    except Exception as exc:  # noqa: BLE001
        log.warning("outputs_cleanup_failed", error=str(exc))

    log.info("tender_processing_done", tender_id=tender_id, s3_uri=s3_uri)


def main() -> None:
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    tender_id = os.getenv("TENDER_ID")
    if not tender_id:
        log.error("missing_tender_id", hint="Set TENDER_ID env variable")
        sys.exit(1)
    asyncio.run(process_tender(tender_id))


if __name__ == "__main__":
    main()
