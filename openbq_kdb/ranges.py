# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Date-range arithmetic for the read-through cache.

Pure functions, no I/O. This is what decides that a 1y->3y zoom fetches two
years rather than three, so it carries the bulk of the extension's tests.

Ranges are (start, end) with BOTH ends inclusive. `step` is one bar width: two
ranges separated by exactly one step are adjacent and coalesce, and a gap's
boundary is pulled in by one step so it does not re-request a covered bar.
"""

from datetime import datetime, timedelta

Range = tuple[datetime, datetime]

_STEPS = {
    "s": timedelta(seconds=1),
    "m": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
}


def interval_step(interval: str) -> timedelta:
    """One bar width for an OpenBB interval string ('1d', '5m', '1h')."""
    import re

    m = re.fullmatch(r"(\d*)\s*([a-zA-Z]+)", str(interval).strip())
    if not m:
        raise ValueError(f"Could not parse interval {interval!r}.")
    n = int(m.group(1) or 1)
    unit = m.group(2).lower()
    if unit in ("mo", "mon", "month", "months"):
        return timedelta(days=30 * n)
    base = _STEPS.get(unit[0]) if unit[0] in _STEPS else None
    if base is None:
        raise ValueError(f"Unsupported interval {interval!r}.")
    return base * n


def coalesce(ranges: list[Range], step: timedelta = timedelta(0)) -> list[Range]:
    """Sort and merge ranges that overlap, or that sit within one `step`.

    With `step` given, ranges one bar apart (Jan 31 / Feb 1 for daily bars) are
    treated as contiguous — otherwise coverage would fragment into thousands of
    single-bar entries and every request would look like a gap.
    """
    if not ranges:
        return []
    ordered = sorted(ranges)
    out = [ordered[0]]
    for start, end in ordered[1:]:
        last_start, last_end = out[-1]
        if start - step <= last_end:
            out[-1] = (last_start, max(last_end, end))
        else:
            out.append((start, end))
    return out


def subtract(requested: Range, covered: list[Range], step: timedelta) -> list[Range]:
    """Return the parts of `requested` not already present in `covered`."""
    req_start, req_end = requested
    if req_start > req_end:
        return []
    gaps: list[Range] = []
    cursor = req_start
    for cov_start, cov_end in coalesce(covered, step):
        if cov_end < cursor:
            continue
        if cov_start > req_end:
            break
        if cov_start > cursor:
            gaps.append((cursor, min(cov_start - step, req_end)))
        cursor = max(cursor, cov_end + step)
        if cursor > req_end:
            return gaps
    if cursor <= req_end:
        gaps.append((cursor, req_end))
    return [(s, e) for s, e in gaps if s <= e]


def trim_tail(r: Range, boundary: datetime) -> Range | None:
    """Clip a range at the last COMPLETED bar boundary.

    Coverage is never recorded past `boundary`, so the still-forming bar is
    refetched on every request instead of being cached half-built.
    """
    start, end = r
    if start > boundary:
        return None
    return (start, min(end, boundary))
