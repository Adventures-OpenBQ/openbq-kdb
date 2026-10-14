# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0

"""Range arithmetic: the logic that decides what actually gets fetched."""

from datetime import datetime, timedelta

import pytest

from openbq_kdb.ranges import coalesce, interval_step, subtract, trim_tail

D = lambda s: datetime.fromisoformat(s)  # noqa: E731
DAY = timedelta(days=1)


def test_coalesce_merges_overlapping():
    out = coalesce([(D("2025-01-01"), D("2025-03-01")), (D("2025-02-01"), D("2025-04-01"))])
    assert out == [(D("2025-01-01"), D("2025-04-01"))]


def test_coalesce_merges_adjacent_given_a_step():
    """Jan 31 and Feb 1 are one bar apart: touching, so they merge."""
    out = coalesce(
        [(D("2025-01-01"), D("2025-01-31")), (D("2025-02-01"), D("2025-02-28"))], DAY
    )
    assert out == [(D("2025-01-01"), D("2025-02-28"))]


def test_coalesce_without_a_step_requires_real_overlap():
    out = coalesce([(D("2025-01-01"), D("2025-01-31")), (D("2025-02-01"), D("2025-02-28"))])
    assert len(out) == 2


def test_coalesce_keeps_disjoint_sorted():
    out = coalesce([(D("2025-06-01"), D("2025-06-30")), (D("2025-01-01"), D("2025-01-31"))])
    assert out == [(D("2025-01-01"), D("2025-01-31")), (D("2025-06-01"), D("2025-06-30"))]


def test_subtract_nothing_covered_is_full_request():
    req = (D("2024-01-01"), D("2025-01-01"))
    assert subtract(req, [], DAY) == [req]


def test_subtract_fully_covered_is_empty():
    req = (D("2024-06-01"), D("2024-12-01"))
    covered = [(D("2024-01-01"), D("2025-01-01"))]
    assert subtract(req, covered, DAY) == []


def test_subtract_returns_only_the_missing_prefix():
    """The 1y -> 3y zoom: two years cached, three requested, one gap fetched."""
    req = (D("2022-01-01"), D("2025-01-01"))
    covered = [(D("2024-01-01"), D("2025-01-01"))]
    gaps = subtract(req, covered, DAY)
    assert gaps == [(D("2022-01-01"), D("2023-12-31"))]


def test_subtract_returns_interior_hole():
    req = (D("2024-01-01"), D("2024-12-31"))
    covered = [(D("2024-01-01"), D("2024-03-31")), (D("2024-07-01"), D("2024-12-31"))]
    assert subtract(req, covered, DAY) == [(D("2024-04-01"), D("2024-06-30"))]


def test_subtract_ignores_coverage_outside_the_request():
    req = (D("2024-01-01"), D("2024-06-30"))
    covered = [(D("2023-01-01"), D("2023-06-30"))]
    assert subtract(req, covered, DAY) == [req]


def test_trim_tail_drops_incomplete_region():
    r = (D("2024-01-01"), D("2025-06-10"))
    assert trim_tail(r, D("2025-06-09")) == (D("2024-01-01"), D("2025-06-09"))


def test_trim_tail_returns_none_when_wholly_incomplete():
    assert trim_tail((D("2025-06-10"), D("2025-06-11")), D("2025-06-09")) is None


def test_trim_tail_leaves_older_range_untouched():
    r = (D("2024-01-01"), D("2024-02-01"))
    assert trim_tail(r, D("2025-06-09")) == r


@pytest.mark.parametrize(
    "interval,expected",
    [("1d", timedelta(days=1)), ("1m", timedelta(minutes=1)),
     ("5m", timedelta(minutes=5)), ("1h", timedelta(hours=1))],
)
def test_interval_step(interval, expected):
    assert interval_step(interval) == expected


def test_interval_step_rejects_unknown():
    with pytest.raises(ValueError):
        interval_step("1fortnight")
