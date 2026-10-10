"""Turn a day's alerts into one message and send it via Telegram and/or email.

Nothing is sent unless --send is given AND credentials are configured; otherwise the message
is printed, so a demo or CI run can never message anyone.
"""
from __future__ import annotations

import smtplib
from datetime import date
from email.message import EmailMessage
from urllib.parse import unquote, urlparse

import pandas as pd
import requests

from imp.config import Settings
from imp.db import query_df

DISCLAIMER = "Statistical signals for research, not evidence of wrongdoing or investment advice."


def fetch(dsn: str, d: date, min_score: int) -> pd.DataFrame:
    return query_df(dsn, "select symbol, rule, score, reason from raw.surveillance_alerts "
                         "where trade_date = %s and score >= %s order by score desc, symbol", (d, min_score))


def format_message(d: date, alerts: pd.DataFrame, limit: int = 15) -> str:
    if alerts.empty:
        return f"Market surveillance {d:%d %b %Y}: no alerts above threshold."
    lines = [f"Market surveillance {d:%d %b %Y}: {alerts['symbol'].nunique()} stocks flagged", ""]
    for sym, g in list(alerts.groupby("symbol", sort=False))[:limit]:
        top = g.iloc[0]
        extra = f" (+{len(g) - 1} more signals)" if len(g) > 1 else ""
        lines.append(f"• {sym} [{int(top.score)}] {top.reason}{extra}")
    lines += ["", DISCLAIMER]
    return "\n".join(lines)


def send_telegram(token: str, chat_id: str, text: str) -> None:
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=20)
    r.raise_for_status()


def send_email(smtp_url: str, to: str, subject: str, text: str) -> None:
    u = urlparse(smtp_url)  # smtp://user:pass@host:587
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, unquote(u.username or ""), to
    msg.set_content(text)
    with smtplib.SMTP(u.hostname, u.port or 587, timeout=30) as s:
        s.starttls()
        if u.username:
            s.login(unquote(u.username), unquote(u.password or ""))
        s.send_message(msg)


def dispatch(cfg: Settings, d: date, send: bool = False) -> dict:
    alerts = fetch(cfg.pg_dsn, d, cfg.min_alert_score)
    text = format_message(d, alerts)
    channels = []
    if send and not alerts.empty:
        if cfg.telegram_token and cfg.telegram_chat_id:
            send_telegram(cfg.telegram_token, cfg.telegram_chat_id, text)
            channels.append("telegram")
        if cfg.smtp_url and cfg.alert_email_to:
            send_email(cfg.smtp_url, cfg.alert_email_to, f"Market surveillance {d}", text)
            channels.append("email")
    if not channels:
        print(text)
    return {"date": d, "alerts": len(alerts), "sent_via": channels or ["stdout"]}
