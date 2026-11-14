"""Bounded observer-owned batches; serialized readers retain all their checks."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping
from copy import deepcopy

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples


class SampleBatch:
    """Receive every completed sample before its parent can discard it.

    One context owns a fixed source closure and at most ``limit`` pending roots.
    Only declaration bytes and exact source bindings cross flushes; compiled
    contexts belong to each existing reader call. Evidence is complete only
    after context exit succeeds. Children may also occur within another root:
    retaining both preserves checks when that parent fails before it emits.
    """

    def __init__(self, operations, *, limit, include_descendants=False):
        if type(limit) is not int or limit < 1:
            raise ValueError("growth batch requires a positive root admission")
        if (
            not isinstance(operations, tuple)
            or not operations
            or len(set(operations)) != len(operations)
        ):
            raise ValueError("growth batch requires distinct source operations")
        if type(include_descendants) is not bool:
            raise ValueError("growth descendant admission requires an explicit boolean")
        self.operations, self.limit = operations, limit
        self.include_descendants = include_descendants
        self.state = "new"
        self.validated_samples = self.validation_calls = 0
        self._pending, self._source = [], []
        self._roots, self._digests = {}, {}
        self._sources = {}
        self._projection_bytes = None

    def __enter__(self):
        if self.state != "new":
            raise ValueError("growth batch has already entered its original scope")
        self.state = "active"
        try:
            roots = tuple(growth_binding(operation) for operation in self.operations)
            projection = project_bindings(roots)
            self._projection_bytes = canonical_json_bytes(projection)
            pending, seen = list(roots), set()
            while pending:
                binding = pending.pop()
                if binding.operation in seen:
                    continue
                seen.add(binding.operation)
                expected = hashlib.sha256(
                    canonical_json_bytes(projection["declarations"][binding.declaration.id])
                ).digest()
                entry = binding, binding.operation, binding.declaration, binding.delegates, expected
                self._source.append(entry)
                self._sources[binding.operation] = entry
                self._digests[binding.operation] = expected.hex()
                pending.extend(growth_binding(operation) for _, operation in binding.delegates)
            self._roots = (
                {operation: entry[0] for operation, entry in self._sources.items()}
                if self.include_descendants
                else {binding.operation: binding for binding in roots}
            )
            self._current()
        except BaseException:
            self._release()
            raise
        return self

    def source(self, operation):
        """Read admitted metadata without hashing inside primitive observers.

        Metadata lookup grants no new receiving authority. Full declaration
        contents are checked at each receiving boundary, including empty exit.
        """
        if self.state != "active":
            raise ValueError("growth source is outside its active original scope")
        try:
            if operation not in self._sources:
                raise ValueError("growth source has no admitted batch owner")
            binding, original, declaration, delegates, _expected = self._sources[operation]
            if (
                growth_binding(operation) is not binding
                or binding.operation is not original
                or binding.declaration is not declaration
                or binding.delegates is not delegates
            ):
                raise ValueError("growth batch source binding or declaration changed")
            return binding, self._digests[operation]
        except BaseException:
            self.state = "failed"
            raise

    def _current(self):
        if self.state != "active":
            raise ValueError("growth batch is outside its active original scope")
        for binding, operation, declaration, delegates, expected in self._source:
            if (
                growth_binding(operation) is not binding
                or binding.operation is not operation
                or binding.declaration is not declaration
                or binding.delegates is not delegates
                or hashlib.sha256(canonical_json_bytes(binding.project())).digest() != expected
            ):
                raise ValueError("growth batch source binding or declaration changed")

    def sample(self, operation, dimensions, metrics, children=None):
        if self.state != "active":
            raise ValueError("growth sample has no active admitted batch owner")
        try:
            if operation not in self._roots:
                raise ValueError("growth sample has no active admitted batch owner")
            if not isinstance(dimensions, Mapping) or not isinstance(metrics, Mapping):
                raise ValueError("growth sample requires actual dimension and metric mappings")
            binding, digest = self.source(operation)
            observed = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": dict(dimensions),
                "metrics": {
                    bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds
                },
                "delegates": {} if children is None else children,
            }
            return self.receive(operation, observed)
        except BaseException:
            self.state = "failed"
            raise

    def receive(self, operation, observed):
        """Receive an exact completed record after primitive observation stops."""
        if self.state != "active":
            raise ValueError("growth sample has no active admitted batch owner")
        try:
            if operation not in self._roots:
                raise ValueError("growth sample has no active admitted batch owner")
            self.source(operation)
            if not isinstance(observed, Mapping):
                raise ValueError("growth sample requires an actual record mapping")
            # This is an identity seal, never a successful-validation receipt.
            seal = hashlib.sha256(canonical_json_bytes(observed)).digest()
            self._pending.append((operation, observed, seal))
            if len(self._pending) == self.limit:
                self.flush()
            return observed
        except BaseException:
            self.state = "failed"
            raise

    def _projection(self, operation, snapshot):
        # Include every declared edge, even when no bound evaluates that edge.
        pending, declarations = [operation], {}
        while pending:
            _binding, _operation, declaration, delegates, _expected = self._sources[pending.pop()]
            if declaration.id in declarations:
                continue
            declarations[declaration.id] = snapshot["declarations"][declaration.id]
            pending.extend(child for _tag, child in delegates)
        # Readers get private nested mappings; mutations cannot leak between
        # groups or flushes. No compiled contexts or per-owner copies are kept.
        return deepcopy({**snapshot, "declarations": declarations})

    def flush(self):
        if self.state != "active":
            raise ValueError("growth batch is outside its active original scope")
        try:
            self._current()
            groups = defaultdict(list)
            for operation, sample, seal in self._pending:
                if hashlib.sha256(canonical_json_bytes(sample)).digest() != seal:
                    raise ValueError("completed growth sample changed before batch receiving")
                groups[operation].append(sample)
            if groups:
                # Read a new private mapping from the immutable admitted bytes.
                snapshot = json.loads(self._projection_bytes)
                for operation, samples in groups.items():
                    validate_execution_samples(
                        self._projection(operation, snapshot),
                        self._roots[operation].declaration.id,
                        samples,
                    )
                    self.validation_calls += 1
                self._current()
                self.validated_samples += len(self._pending)
                self._pending.clear()
        except BaseException:
            self.state = "failed"
            raise

    def _release(self):
        self._pending.clear()
        self._source.clear()
        self._roots.clear()
        self._digests.clear()
        self._sources.clear()
        self._projection_bytes = None
        self.state = "closed"

    def __exit__(self, _kind, _error, _traceback):
        try:
            if self.state == "active":
                self.flush()
            elif _error is None:
                raise ValueError("growth batch cannot receive a previously failed scope")
        finally:
            self._release()
        return False
