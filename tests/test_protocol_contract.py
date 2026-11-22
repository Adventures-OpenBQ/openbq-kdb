# Copyright 2026 SecretoftheUniverse.com LLC. Licensed under the Apache License, Version 2.0.
# SPDX-License-Identifier: Apache-2.0
"""Shared browser fixtures must be consumable without losing cursor precision."""
from importlib.resources import files
import hashlib
import json

from jsonschema import Draft202012Validator
import pytest


def contract():
    root = files("openbq_kdb").joinpath("resources")
    schema = json.loads(root.joinpath("chart-stream-v1.schema.json").read_text())
    traces = json.loads(root.joinpath("chart-stream-v1.examples.json").read_text())
    return Draft202012Validator(schema), traces


def test_shared_transcripts_validate_on_both_sides_of_the_wire():
    validator, traces = contract()
    for trace in traces:
        for frame in trace["frames"]:
            validator.validate(frame)


def test_example_checkpoints_are_bound_to_the_advertised_normalized_view():
    _, traces = contract()
    for trace in traces:
        for frame in trace["frames"]:
            if frame["type"] in {"snapshot_begin", "resumed", "resume"}:
                encoded = json.dumps(frame["view"], sort_keys=True, separators=(",", ":"))
                wanted = hashlib.sha256(encoded.encode("ascii")).hexdigest()
                actual = frame.get("view_key") or frame["checkpoint"]["view_key"]
                assert actual == wanted


@pytest.mark.parametrize("mutation", ["numeric_cursor", "missing_generation", "unknown_type",
                                     "extra_query", "numeric_tick_id"])
def test_wire_rejects_lossy_or_unscoped_frames(mutation):
    validator, traces = contract()
    frame = next(f for t in traces for f in t["frames"] if f["type"] == "ticks")
    frame = json.loads(json.dumps(frame))
    if mutation == "numeric_cursor":
        frame["through_cursor"]["seq"] = 9007199254740993
    elif mutation == "missing_generation":
        del frame["generation"]
    elif mutation == "unknown_type":
        frame["type"] = "eval"
    elif mutation == "extra_query":
        frame["query"] = "1+1"
    else:
        frame["batches"][0]["ticks"][0]["tick_id"]["seq"] = 9007199254740993
    assert not validator.is_valid(frame)
