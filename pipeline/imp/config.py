"""Runtime configuration, read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "ITC", "LT", "BHARTIARTL",
                   "MARUTI", "MRF", "PAGEIND", "YESBANK", "SUZLON", "IRFC", "ADANIENT", "ZOMATO", "BAJFINANCE"]


def _list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    return [s.strip().upper() for s in raw.split(",") if s.strip()] or default


@dataclass(frozen=True)
class Settings:
    lake_root: Path = field(default_factory=lambda: Path(os.getenv("LAKE_ROOT", "lake")))
    pg_dsn: str = field(default_factory=lambda: os.getenv(
        "PG_DSN", "postgresql://imp:imp@localhost:5432/market"))
    symbols: list[str] = field(default_factory=lambda: _list("SYMBOLS", DEFAULT_SYMBOLS))
    mf_schemes: list[str] = field(default_factory=lambda: _list("MF_SCHEMES", []))  # empty = all schemes
    telegram_token: str | None = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN") or None)
    telegram_chat_id: str | None = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID") or None)
    smtp_url: str | None = field(default_factory=lambda: os.getenv("SMTP_URL") or None)  # smtp://user:pass@host:587
    alert_email_to: str | None = field(default_factory=lambda: os.getenv("ALERT_EMAIL_TO") or None)
    min_alert_score: int = field(default_factory=lambda: int(os.getenv("MIN_ALERT_SCORE", "60")))

    @property
    def bronze(self) -> Path:
        return self.lake_root / "bronze"

    @property
    def silver(self) -> Path:
        return self.lake_root / "silver"


def settings() -> Settings:
    return Settings()
