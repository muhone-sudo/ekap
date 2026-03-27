"""Pydantic v2 schemas shared across services."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class TenderStatus(str, Enum):
    PENDING = "PENDING"
    DOWNLOADED = "DOWNLOADED"
    UPLOADED = "UPLOADED"
    FAILED = "FAILED"


class ManifestMessage(BaseModel):
    """SQS manifest message published by Radar after a successful upload."""

    tender_id: str = Field(..., description="EKAP ihale numarası")
    s3_uri: str = Field(..., description="s3://<bucket>/<key> URI")
    file_size_bytes: int = Field(..., ge=0)
    status: TenderStatus = TenderStatus.UPLOADED
    timestamp_utc: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    correlation_id: str = Field(default="")

    @model_validator(mode="after")
    def s3_uri_must_start_with_s3(self) -> "ManifestMessage":
        if not self.s3_uri.startswith("s3://"):
            raise ValueError("s3_uri must start with 's3://'")
        return self

    def model_dump_json_str(self) -> str:
        return self.model_dump_json()


class FinancialRatios(BaseModel):
    """Mali yeterlilik rasyoları (Madde 7)."""

    current_ratio: float = Field(..., alias="C_o", description="Cari oran >= 0.70")
    equity_ratio: float = Field(..., alias="O_o", description="Öz kaynak oranı >= 0.10")
    debt_ratio: float = Field(..., alias="B_o", description="Borç oranı < 0.50")
    confidence: float = Field(..., ge=0.0, le=1.0)
    requires_hitl: bool = False

    @model_validator(mode="after")
    def check_hitl_threshold(self) -> "FinancialRatios":
        self.requires_hitl = self.confidence < 0.70
        return self

    def meets_criteria(self) -> bool:
        return (
            self.current_ratio >= 0.70
            and self.equity_ratio >= 0.10
            and self.debt_ratio < 0.50
        )

    class Config:
        populate_by_name = True


class ScraperConfig(BaseModel):
    """Runtime configuration for the Radar scraper."""

    headless: bool = True
    jitter_min: float = Field(1.5, gt=0)
    jitter_max: float = Field(4.5, gt=0)
    block_threshold: float = Field(0.15, ge=0.0, le=1.0)
    s3_bucket: str
    sqs_queue_url: str
    ekap_base_url: str = "https://ekap.kik.gov.tr/EKAP/"

    @model_validator(mode="after")
    def jitter_order(self) -> "ScraperConfig":
        if self.jitter_min >= self.jitter_max:
            raise ValueError("jitter_min must be less than jitter_max")
        return self
