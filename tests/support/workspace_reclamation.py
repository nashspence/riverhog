"""Observe actual extent syscalls and exact delegated reclamation invocations."""

from __future__ import annotations

import builtins
import hashlib
import io
import os
import sqlite3
from contextlib import contextmanager

import pytest
import workspace_reclamation
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding

_FSENCODE = os.fsencode


def execution_sample(binding, projection, dimensions, metrics, delegates):
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dict(dimensions),
        "metrics": dict(metrics),
        "delegates": delegates,
    }


@contextmanager
def observe_reclamation(monkeypatch, operation):
    binding = growth_binding(operation)
    metrics = {bound.metric: 0 for bound in binding.declaration.bounds}
    names = ("stat", "fstat", "getuid", "open", "close", "ftruncate", "unlink", "fsync")
    originals = {name: getattr(os, name) for name in names}
    compare = workspace_reclamation._compare_names

    def counted(name, metric):
        original = originals[name]

        def observed(*args, **kwargs):
            metrics[metric] += 1
            if name == "ftruncate":
                before = originals["fstat"](args[0]).st_size
                result = original(*args, **kwargs)
                metrics["reclaimed_extent_octets"] += before - originals["fstat"](args[0]).st_size
                return result
            if name == "unlink":
                before = originals["stat"](args[0], follow_symlinks=False, **kwargs).st_size
                result = original(*args, **kwargs)
                metrics["reclaimed_extent_octets"] += before
                return result
            return original(*args, **kwargs)

        return observed

    def encoded(name):
        value = _FSENCODE(name)
        metrics["encoded_name_octets"] += len(value)
        return value

    def compared(left, right, **kwargs):
        metrics["name_order_comparisons"] += 1
        metrics["name_comparison_octets"] += len(left) + len(right)
        return compare(left, right, **kwargs)

    def forbidden(*_args, **_kwargs):
        pytest.fail("reclamation read payloads, opened SQLite or enumerated a directory")

    with monkeypatch.context() as patch:
        for name, metric in (
            ("stat", "metadata_calls"),
            ("fstat", "metadata_calls"),
            ("getuid", "owner_identity_calls"),
            ("open", "open_calls"),
            ("close", "close_calls"),
            ("ftruncate", "truncate_calls"),
            ("unlink", "unlink_calls"),
            ("fsync", "sync_calls"),
        ):
            patch.setattr(os, name, counted(name, metric))
        patch.setattr(os, "fsencode", encoded)
        patch.setattr(workspace_reclamation, "_compare_names", compared)
        for module, name in (
            (builtins, "open"),
            (io, "open"),
            (os, "read"),
            (os, "listdir"),
            (os, "scandir"),
            (sqlite3, "connect"),
        ):
            patch.setattr(module, name, forbidden)
        yield metrics


@contextmanager
def observe_file_delegate(monkeypatch, module, projection, dimensions, metrics):
    actual = module.reclaim_retired_files
    child = growth_binding(actual)
    samples = []

    def observed(directory, *, names, octet_budget):
        before = dict(metrics)
        result = actual(directory, names=names, octet_budget=octet_budget)
        encoded = tuple(_FSENCODE(name) for name in names)
        samples.append(
            execution_sample(
                child,
                projection,
                {
                    "candidate_files": len(names),
                    "name_octets": sum(map(len, encoded)),
                    "maximum_name_octets": max(map(len, encoded), default=0),
                    **dimensions,
                    "octet_budget": octet_budget,
                },
                {name: metrics[name] - before[name] for name in metrics},
                {},
            )
        )
        return result

    with monkeypatch.context() as patch:
        patch.setattr(module, "reclaim_retired_files", observed)
        yield samples
