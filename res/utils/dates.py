from datetime import datetime, timedelta, date
from dateutil.parser import parse as date_parse
import pytz


SECONDS_IN_DAY = 86400


def coerce_to_full_datetime(d, tzinfo=None):
    if isinstance(d, str):
        d = date_parse(d)

    if isinstance(d, date):
        d = datetime.combine(d, datetime.min.time())

    return d.replace(tzinfo=tzinfo)


def parse(s, tz_aware=True):
    dt = date_parse(s)

    if not tz_aware:
        dt = dt.replace(tzinfo=None)

    return dt


def utc_now(tzinfo=pytz.utc):
    """Pass none to remove the time zone info"""
    dt = datetime.utcnow()
    return dt.replace(tzinfo=tzinfo)


def utc_iso_string_of(dt):
    return dt.replace(tzinfo=None).isoformat()


def utc_hours_ago(hours):
    return utc_now() - timedelta(hours=hours)


def utc_days_ago(days):
    return utc_now() - timedelta(days=days)


def utc_minutes_ago(minutes):
    return utc_now() - timedelta(minutes=minutes)


def utc_now_iso_string(tzinfo=pytz.utc):
    return utc_now(tzinfo=tzinfo).isoformat()


def relative_to_now(watermark, reverse=True, tzinfo=None):
    if watermark:
        watermark = watermark if not reverse else watermark * -1
        return utc_now(tzinfo).date() + timedelta(days=watermark)
    return None


def minutes_ago(watermark, reverse=True, tzinfo=None):
    if watermark:
        watermark = watermark if not reverse else watermark * -1
        return utc_now(tzinfo) + timedelta(minutes=watermark)
    return None


def time_diff_in_seconds(minuend, subtrahend):
    delta = minuend - subtrahend
    return delta.total_seconds()
