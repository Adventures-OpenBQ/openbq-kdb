# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Aggregating ticks into OHLCV buckets."""

from datetime import datetime

import pandas as pd
import pytest

from openbq_kdb.aggregate import aggregate_ticks, bucket_ns

D = lambda s: datetime.fromisoformat(s)  # noqa: E731


class FakeStore:
    """Returns a canned aggregation frame and records the call."""

    def __init__(self, frame=None):
        self.frame = frame if frame is not None else pd.DataFrame()
        self.calls = []

    def aggregate_frame(self, symbol, interval, start, end):
        self.calls.append((symbol, interval, start, end))
        return self.frame


def test_bucket_ns_for_supported_intervals():
    assert bucket_ns("1m") == 60_000_000_000
    assert bucket_ns("5m") == 300_000_000_000
    assert bucket_ns("1h") == 3_600_000_000_000
    assert bucket_ns("1d") == 86_400_000_000_000


def test_bucket_ns_keeps_every_width_the_old_table_held():
    """The seven `bucket_ns` served before it became a parser, unchanged.

    A drift here would silently re-bucket every chart on the platform, so the
    numbers are written out rather than computed from the same units the
    implementation uses.
    """
    assert bucket_ns("1s") == 1_000_000_000
    assert bucket_ns("1m") == 60_000_000_000
    assert bucket_ns("5m") == 300_000_000_000
    assert bucket_ns("15m") == 900_000_000_000
    assert bucket_ns("30m") == 1_800_000_000_000
    assert bucket_ns("1h") == 3_600_000_000_000
    assert bucket_ns("1d") == 86_400_000_000_000


def test_bucket_ns_takes_any_fixed_width_interval():
    """The point of the change: `xbar` takes an integer width, so an interval
    nobody put in a table costs exactly what a familiar one costs."""
    assert bucket_ns("30s") == 30_000_000_000
    assert bucket_ns("7m") == 420_000_000_000
    assert bucket_ns("90m") == 5_400_000_000_000
    assert bucket_ns("4h") == 14_400_000_000_000
    assert bucket_ns("2d") == 172_800_000_000_000


def test_bucket_ns_ignores_surrounding_whitespace():
    assert bucket_ns("  90m  ") == 5_400_000_000_000


def test_bucket_ns_rejects_unknown():
    with pytest.raises(ValueError):
        bucket_ns("1fortnight")


@pytest.mark.parametrize("interval", ["1mo", "1w", "1y"])
def test_bucket_ns_refuses_intervals_that_are_not_a_constant_width(interval):
    """A month is not a fixed number of nanoseconds, so `xbar` cannot bucket
    on one. Refusing is what routes these to the history path, which serves
    them properly -- see `tick_capable` in live-grid's app.series."""
    with pytest.raises(ValueError):
        bucket_ns(interval)


def test_bucket_ns_is_case_sensitive_so_a_minute_is_never_a_month():
    """`1m` is a minute and `1M` a month. Folding case here would turn a
    monthly request into a one-minute chart."""
    assert bucket_ns("1m") == 60_000_000_000
    with pytest.raises(ValueError):
        bucket_ns("1M")


@pytest.mark.parametrize("interval", ["0m", "0s", "", "m", "-5m", "5", "1.5m", "5 m"])
def test_bucket_ns_refuses_malformed_intervals(interval):
    """A zero width would make `xbar` divide by zero; the rest are not
    intervals at all."""
    with pytest.raises(ValueError):
        bucket_ns(interval)


def test_aggregate_maps_q_columns_to_bar_rows():
    frame = pd.DataFrame({
        "t": [pd.Timestamp("2025-06-10T14:00:00")],
        "open": [100.0], "high": [103.0], "low": [100.0],
        "close": [101.0], "volume": [16.0], "vwap": [101.5],
    })
    rows = aggregate_ticks(
        FakeStore(frame), "AAPL", "1m", D("2025-06-10T14:00"), D("2025-06-10T15:00")
    )
    assert rows == [{
        "date": pd.Timestamp("2025-06-10T14:00:00"),
        "open": 100.0, "high": 103.0, "low": 100.0, "close": 101.0, "volume": 16.0,
        "vwap": 101.5,
    }]


def test_aggregate_nulls_vwap_when_the_bucket_had_no_sized_ticks():
    """size wavg price over all-zero sizes is q null (NaN) -- 'no trade
    data', never a fabricated number."""
    frame = pd.DataFrame({
        "t": [pd.Timestamp("2025-06-10T14:00:00")],
        "open": [100.0], "high": [103.0], "low": [100.0],
        "close": [101.0], "volume": [0.0], "vwap": [float("nan")],
    })
    rows = aggregate_ticks(
        FakeStore(frame), "AAPL", "1m", D("2025-06-10T14:00"), D("2025-06-10T15:00")
    )
    assert rows[0]["vwap"] is None


def test_aggregate_returns_empty_list_for_no_ticks():
    assert aggregate_ticks(FakeStore(), "AAPL", "1m", D("2025-01-01"), D("2025-01-02")) == []


def test_aggregate_passes_the_window_through_unchanged():
    store = FakeStore()
    start, end = D("2025-06-10T14:00"), D("2025-06-10T15:00")
    aggregate_ticks(store, "AAPL", "5m", start, end)
    assert store.calls == [("AAPL", "5m", start, end)]


def test_rows_are_time_ordered():
    frame = pd.DataFrame({
        "t": [pd.Timestamp("2025-06-10T14:01:00"), pd.Timestamp("2025-06-10T14:00:00")],
        "open": [99.0, 100.0], "high": [99.0, 103.0], "low": [99.0, 100.0],
        "close": [99.0, 101.0], "volume": [7.0, 16.0],
    })
    rows = aggregate_ticks(
        FakeStore(frame), "AAPL", "1m", D("2025-06-10T14:00"), D("2025-06-10T15:00")
    )
    assert [r["date"] for r in rows] == sorted(r["date"] for r in rows)
