"""
Pydantic v2 schemas for scraped merchant data.

These models validate/coerce data coming out of the scrapers before it is
persisted to PostgreSQL.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class MenuItem(BaseModel):
    """A single item on a merchant's menu."""

    name: str
    price: float | None = None
    description: str | None = None
    category: str | None = None

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price(cls, v):
        """Strip currency symbols and coerce to float."""
        if isinstance(v, str):
            v = v.replace("$", "").replace(",", "").strip()
            return float(v) if v else None
        return v


class MerchantListing(BaseModel):
    """
    Canonical representation of a merchant scraped from one competitor platform.
    """

    platform: str = Field(..., description="Source platform slug, e.g. 'doordash'")
    platform_merchant_id: str = Field(..., description="Platform-specific merchant ID")
    name: str
    address: str = ""
    lat: float | None = None
    lng: float | None = None
    cuisine_tags: list[str] = Field(default_factory=list)
    rating: float | None = None
    review_count: int | None = None
    price_bucket: str | None = None  # "$", "$$", "$$$", "$$$$"
    delivery_fee: float | None = None
    estimated_delivery_min: int | None = None
    is_promoted: bool = False
    menu_items: list[MenuItem] = Field(default_factory=list)
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    market: str = Field(..., description="Market slug, e.g. 'nyc'")
    raw_url: str = ""

    @field_validator("rating", mode="before")
    @classmethod
    def clamp_rating(cls, v):
        if v is not None:
            v = float(v)
            return max(0.0, min(5.0, v))
        return v

    @field_validator("cuisine_tags", mode="before")
    @classmethod
    def ensure_list(cls, v):
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return v or []


class CrawlResult(BaseModel):
    """Summary of a single crawl run."""

    platform: str
    market: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"  # running | success | failed
    merchants_found: int = 0
    errors: int = 0
    error_detail: str | None = None
    listings: list[MerchantListing] = Field(default_factory=list)
