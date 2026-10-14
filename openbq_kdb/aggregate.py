# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Aggregating recorded ticks into OHLCV bars.

The aggregation itself runs in q (`xbar`), next to the data. This module only
maps intervals to bucket widths and the returned frame to bar rows.
"""

import re

#: Fixed-width units `xbar` can bucket on. Months and years are absent on
#: purpose: a bucket is a constant number of nanoseconds and a month is not
#: one. An interval this cannot express (`1mo`, `1w`) is not a failure -- it
#: is served by the history path instead, which is what `tick_capable` in
#: live-grid's app.series decides.
_UNIT_NS = {
    "s": 1_000_000_000,
    "m": 60_000_000_000,
    "h": 3_600_000_000_000,
    "d": 86_400_000_000_000,
}

#: `<count><unit>`, case-sensitive: `1m` is a minute and `1M` would be a
#: month. (On main the same distinction is drawn in app.ta.session; that
#: module does not exist on this line.)
_INTERVAL = re.compile(r"^(\d+)([smhd])$")


def bucket_ns(interval: str) -> int:
    """Bucket width in nanoseconds for an interval q can aggregate.

    Any whole number of seconds, minutes, hours or days. The width reaches q
    as an integer handed to `xbar`, which has no opinion about which widths
    are "standard", so `7m` and `90m` cost exactly what `5m` does.

    This was a seven-entry lookup table (1s/1m/5m/15m/30m/1h/1d) until
    2026-09-25. Nothing about the ticks or about q required that list: it was
    the only thing standing between the tick store and an arbitrary bucket,
    and because `tick_capable` reads a ValueError here as "the tick plane
    cannot express this", an interval outside the seven fell through to the
    history path -- where a provider that could not serve it answered with
    DAILY bars, HTTP 200, and nothing saying the request had been ignored.
    A chart asking for 90m drew daily candles labelled 90m.

    Widening the parser fixes that at the source for every fixed-width
    interval.

    Back-placed to the v10 line, where kdb was introduced, from the change
    first made on main (#75). One hunk of that change does not travel:
    `live-grid/app/ta/session.py` does not exist on this line, so its
    reference to this table needed no update here.
    """
    match = _INTERVAL.match(str(interval).strip())
    if match is None:
        raise ValueError(
            f"Interval {interval!r} cannot be aggregated from ticks. "
            "Expected <count><unit>, unit one of s, m, h, d -- e.g. 30s, 5m, 90m, 4h."
        )
    count = int(match.group(1))
    if count < 1:
        raise ValueError(
            f"Interval {interval!r} must be a positive number of units."
        )
    return count * _UNIT_NS[match.group(2)]


def aggregate_ticks(store, symbol: str, interval: str, start, end) -> list[dict]:
    """OHLCV rows built from the ticks held for `symbol` within [start, end]."""
    import math

    frame = store.aggregate_frame(symbol, interval, start, end)
    if frame is None or getattr(frame, "empty", True):
        return []
    import math

    ordered = frame.sort_values("t")
    rows = []
    for row in ordered.itertuples():
        # vwap is the bar's true trade-weighted price (size wavg price in q).
        # A bucket whose ticks all carry size 0 gives q's null (NaN here) --
        # keep it None: "no trade data", never a fabricated value.
        vwap = float(getattr(row, "vwap", float("nan")))
        rows.append({
            "date": row.t,
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
            "volume": float(row.volume),
            "vwap": None if math.isnan(vwap) else vwap,
        })
    return rows
