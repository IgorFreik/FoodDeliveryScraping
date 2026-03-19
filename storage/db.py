"""
Database connection and ORM models for the scraping pipeline.

Uses SQLAlchemy 2.0 with the psycopg2 driver and GeoAlchemy2 for PostGIS.
"""

from __future__ import annotations

import os
from datetime import datetime

from geoalchemy2 import Geography
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker

# ── Connection ──────────────────────────────────────────────────────

DATABASE_URL = os.getenv(
    "SCRAPER_DB_URL",
    "postgresql+psycopg2://scraper:scraper123@localhost:5432/scraping",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, echo=False)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def get_session() -> Session:
    """Return a new database session."""
    return SessionLocal()


# ── ORM Base ────────────────────────────────────────────────────────

Base = declarative_base()


# ── Models ──────────────────────────────────────────────────────────


class PlatformMerchant(Base):
    __tablename__ = "platform_merchants"
    __table_args__ = (UniqueConstraint("platform", "platform_id", name="uq_platform_merchant"),)

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    platform = Column(String, nullable=False)
    platform_id = Column(String, nullable=False)
    name = Column(Text)
    address = Column(Text)
    geom = Column(Geography("POINT", srid=4326))
    cuisine_tags = Column(ARRAY(Text))
    rating = Column(Numeric(3, 2))
    review_count = Column(Integer)
    price_bucket = Column(String)
    delivery_fee = Column(Numeric(6, 2))
    estimated_delivery_min = Column(Integer)
    is_promoted = Column(Boolean, default=False)
    market = Column(String)
    scraped_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    raw_s3_key = Column(Text)
    raw_url = Column(Text)

    menu_items = relationship(
        "MenuItemRow", back_populates="merchant", cascade="all, delete-orphan"
    )
    match = relationship("MerchantMatch", back_populates="platform_merchant", uselist=False)


class Merchant(Base):
    __tablename__ = "merchants"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    canonical_name = Column(Text)
    canonical_geom = Column(Geography("POINT", srid=4326))
    canonical_addr = Column(Text)
    is_on_our_platform = Column(Boolean, default=False)
    our_merchant_id = Column(String)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    matches = relationship("MerchantMatch", back_populates="merchant")


class MerchantMatch(Base):
    __tablename__ = "merchant_matches"

    platform_merchant_id = Column(
        BigInteger, ForeignKey("platform_merchants.id", ondelete="CASCADE"), primary_key=True
    )
    merchant_id = Column(BigInteger, ForeignKey("merchants.id", ondelete="CASCADE"), nullable=False)
    confidence = Column(Numeric(4, 3))
    match_method = Column(String)
    matched_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    platform_merchant = relationship("PlatformMerchant", back_populates="match")
    merchant = relationship("Merchant", back_populates="matches")


class MenuItemRow(Base):
    __tablename__ = "menu_items"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    platform_merchant_id = Column(
        BigInteger, ForeignKey("platform_merchants.id", ondelete="CASCADE"), nullable=False
    )
    name = Column(Text, nullable=False)
    price = Column(Numeric(8, 2))
    description = Column(Text)
    category = Column(String)
    scraped_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    merchant = relationship("PlatformMerchant", back_populates="menu_items")


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    platform = Column(String, nullable=False)
    market = Column(String, nullable=False)
    started_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    finished_at = Column(DateTime(timezone=True))
    status = Column(String, default="running")
    merchants_found = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    error_detail = Column(Text)
