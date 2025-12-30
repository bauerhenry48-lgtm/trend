"""Utility functions for datetime and data manipulation."""

from datetime import datetime, timezone
import pandas as pd


def convert_to_datetime(dt) -> datetime:
    """Convert pd.Timestamp or datetime to datetime."""
    if isinstance(dt, pd.Timestamp):
        return dt.to_pydatetime()
    return dt


def round_to_30_minutes(dt: datetime) -> datetime:
    """Round datetime down to nearest 30-minute mark."""
    dt = convert_to_datetime(dt)
    rounded_minutes = 30 if dt.minute >= 30 else 0
    return dt.replace(minute=rounded_minutes, second=0, microsecond=0)

