"""Bounded immutable envelopes tied to the original complete page-map root."""

from __future__ import annotations

import hashlib
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from ._generation_page_map import GenerationPageMap, InvalidGenerationMap
from ._native_work import WorkAmount
from .errors import JournalValidationCapacity

if TYPE_CHECKING:
    from ._generation_storage import NativeGenerationStorage

_DOMAIN = b"riverhog-native-generation-envelope/v1\x00"


@dataclass(slots=True)
class GenerationEnvelopeWork:
    sql_calls: int = 0
    loaded_octets: int = 0
    stored_octets: int = 0
    hashed_octets: int = 0


def envelope_digest(body: bytes) -> str:
    return hashlib.sha256(_DOMAIN + body).hexdigest()


def _growth(mode: str) -> Declaration:
    units = Expression("dimension", name="units")
    width = Expression("dimension", name="metadata_octets")

    def scaled_width(extra: int) -> Expression:
        return Expression(
            "product",
            terms=(
                units,
                Expression(
                    "sum",
                    terms=(
                        width,
                        Expression("constant", value=extra, unit=OCTETS_PER_COUNT),
                    ),
                ),
            ),
        )

    return Declaration(
        id=f"riverhog-provenance.generation-envelope-{mode}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._generation_envelope.GenerationEnvelopes.{mode}",),
        scope="exact-original-native-generation-envelope",
        starts_at="receive original admitted metadata and exact root identity",
        ends_at="receive the immutable envelope or preserve its original failure",
        dimensions=(
            Dimension("units", COUNT, "One exact envelope operation attempted."),
            Dimension(
                "metadata_octets", OCTETS_PER_COUNT, "Original admitted envelope record width."
            ),
        ),
        bounds=(
            Bound("sql_calls", COUNT, "upper", units, "One exact indexed row operation."),
            Bound(
                "loaded_octets",
                OCTETS,
                "upper",
                scaled_width(66 if mode == "read" else 0),
                "Bounded body and digest with one refusal byte per projected field.",
            ),
            Bound(
                "stored_octets",
                OCTETS,
                "upper",
                scaled_width(0) if mode == "seal" else Expression("constant", value=0, unit=OCTETS),
                "One admitted envelope record; reads publish nothing.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                scaled_width(len(_DOMAIN)),
                "One domain-separated digest of the admitted body.",
            ),
        ),
        conditions=(
            "Metadata admission precedes body materialization; counts include attempted work.",
        ),
        invariants=(
            "No history scan, whole-page-map traversal or mutable envelope replacement.",
            "Retirement removes envelope and root in the same original row operation.",
        ),
        limitations=(
            "SQL call bounds exclude B-tree VM instructions, physical I/O and synchronization.",
            "This metadata operation establishes no source authority or Core selection.",
        ),
    )


class GenerationEnvelopes:
    """One immutable bounded envelope per root row, retired with that same row.

    The journal/corpus owner encodes its complete native control and opaque
    application continuation. This port owns exact root/digest binding and
    bounded materialization; it grants no current source or Core authority.
    """

    def __init__(self, storage: NativeGenerationStorage, *, metadata_octets: int) -> None:
        self.storage, self.metadata_octets = storage, metadata_octets
        self.work = GenerationEnvelopeWork()

    def _require(self) -> None:
        self.storage._require_operation()
        self.storage.epoch.check()

    @declare_growth(_growth("seal"))
    def seal(self, identity: str, root: str, body: bytes) -> str:
        """Attach exactly once inside the original root-freezing transaction."""
        self._require()
        epoch, db = self.storage.epoch, self.storage.db
        epoch.require(db)
        self.storage.roots._identity(identity)
        GenerationPageMap._address(root)
        if type(body) is not bytes:
            raise TypeError("generation envelope requires exact encoded bytes")
        size = len(identity.encode("utf-8")) + 64 + 9 + 64 + len(body)
        if size > self.metadata_octets:
            raise JournalValidationCapacity("optional generation envelope admission exhausted")
        epoch.account(WorkAmount(rows=1, octets=size + len(_DOMAIN) + len(body)))
        self.work.stored_octets += size
        self.work.hashed_octets += len(_DOMAIN) + len(body)
        digest = envelope_digest(body)
        self.work.sql_calls += 1
        with closing(
            db.execute(
                "UPDATE generation_roots SET envelope=?,envelope_sha256=? "
                "WHERE identity=? AND root=? AND status='immutable' AND envelope IS NULL "
                "AND envelope_sha256 IS NULL RETURNING identity",
                (body, digest, identity, root),
            )
        ) as rows:
            row = rows.fetchone()
        if row is not None:
            received = len(identity.encode("utf-8"))
            epoch.account(WorkAmount(octets=received))
            self.work.loaded_octets += received
        if row != (identity,):
            raise InvalidGenerationMap("generation envelope root is changed or already sealed")
        return digest

    @declare_growth(_growth("read"))
    def read(self, identity: str, *, root: str, expected_digest: str) -> bytes:
        """Load only bounded exact metadata; never enumerate generation history."""
        self.storage.roots._identity(identity)
        GenerationPageMap._address(root)
        GenerationPageMap._address(expected_digest)
        with self.storage.execution():
            self._require()
            epoch = self.storage.epoch
            epoch.account(WorkAmount(rows=1))
            self.work.sql_calls += 1
            with closing(
                self.storage.db.execute(
                    "SELECT typeof(envelope),CASE WHEN length(envelope)=0 THEN x'' "
                    "ELSE substr(CAST(envelope AS BLOB),1,?) END,"
                    "typeof(envelope_sha256),substr(CAST(envelope_sha256 AS BLOB),1,65) "
                    "FROM generation_roots WHERE identity=? AND root=? AND status='immutable'",
                    (self.metadata_octets + 1, identity, root),
                )
            ) as rows:
                row = rows.fetchone()
            if row is None:
                raise InvalidGenerationMap("generation envelope is missing")
            body_type, body, digest_type, digest = row
            size = sum(len(value) for value in (body, digest) if type(value) is bytes)
            epoch.account(WorkAmount(octets=size))
            self.work.loaded_octets += size
            if (
                body_type != "blob"
                or type(body) is not bytes
                or len(body) > self.metadata_octets
                or digest_type != "text"
                or digest != expected_digest.encode("ascii")
            ):
                raise InvalidGenerationMap("generation envelope storage or digest differs")
            epoch.account(WorkAmount(octets=len(_DOMAIN) + len(body)))
            self.work.hashed_octets += len(_DOMAIN) + len(body)
            if envelope_digest(body) != expected_digest:
                raise InvalidGenerationMap("generation envelope content differs")
            return body
