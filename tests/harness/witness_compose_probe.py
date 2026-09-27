# SPDX-License-Identifier: Apache-2.0
"""Exercise retained witness evidence in the supported Compose runtime images."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from a_riverhog_witness_contract_lib import CollectionWitnessStatement

STATE = Path("/state/witness.sqlite3")
CALENDAR_URL = "https://calendar.example.invalid"


def _digest_for_collection(collection_id: int) -> str | None:
    with sqlite3.connect(STATE) as db:
        rows = db.execute("SELECT digest, payload FROM statements").fetchall()
    for digest, payload in rows:
        statement = CollectionWitnessStatement.parse(bytes(payload))
        if statement.collection_id == collection_id:
            assert statement.sha256().hex() == digest
            return str(digest)
    return None


def _store(kind: str):
    if kind == "minisign":
        from a_riverhog_minisign_witness.store import WitnessStore

        return WitnessStore(STATE)
    if kind == "opentimestamps":
        from a_riverhog_opentimestamps_witness.store import WitnessStore

        return WitnessStore(STATE, (CALENDAR_URL,))
    raise ValueError("unknown witness kind")


def _ingest_until_observed(kind: str, store: Any, collection_id: int) -> str:
    if kind == "minisign":
        from a_riverhog_minisign_witness.cli import _api_client
    else:
        from a_riverhog_opentimestamps_witness.cli import _api_client
    with _api_client() as api:
        for _ in range(100):
            digest = _digest_for_collection(collection_id)
            if digest is not None:
                return digest
            batch = store.ingest_once(api)
            assert batch.kind != "reset", "authorization view changed during witness probe"
    raise AssertionError("finalized collection was not observed")


def _mature_with_deterministic_calendar(store: Any, digest: str) -> None:
    from a_riverhog_opentimestamps_witness.proof import CalendarError
    from opentimestamps.core.notary import PendingAttestation
    from opentimestamps.core.serialize import BytesSerializationContext
    from opentimestamps.core.timestamp import Timestamp

    class Calendar:
        def request(self, kind: str, url: str, commitment: bytes, max_bytes: int) -> bytes:
            assert url == CALENDAR_URL
            if kind == "upgrade":
                raise CalendarError("not_found", retryable=True)
            assert kind == "submit"
            timestamp = Timestamp(commitment)
            timestamp.attestations.add(PendingAttestation(url))
            context = BytesSerializationContext()
            timestamp.serialize(context)
            result = context.getbytes()
            assert len(result) <= max_bytes
            return result

    for _ in range(100):
        evidence = store.evidence(digest)
        assert evidence is not None
        if evidence["proof"] is not None:
            return
        attempt = store.mature_once(Calendar())
        assert attempt is not None, "proof job was not due"
        if attempt.digest == digest:
            assert attempt.proof_revision_retained
    raise AssertionError("target proof was not retained")


def _sign_with_compose_key(store: Any, digest: str) -> None:
    from a_riverhog_minisign_witness.minisign import MinisignSigner

    signer = MinisignSigner(
        Path("/run/secrets/minisign_secret_key"), Path("/run/secrets/minisign_public_key")
    )
    for _ in range(100):
        evidence = store.evidence(digest)
        assert evidence is not None
        if evidence["signature"] is not None:
            return
        attempt = store.sign_once(signer)
        assert attempt is not None and attempt.outcome == "signed"
    raise AssertionError("target signature was not retained")


def _evidence_hash(kind: str, store: Any, digest: str, collection_id: int) -> str:
    evidence = store.evidence(digest)
    assert evidence is not None
    statement = CollectionWitnessStatement.parse(evidence["statement"])
    assert statement.collection_id == collection_id
    if kind == "minisign":
        assert evidence["state"] == "signed"
        raw = evidence["signature"]
    else:
        assert evidence["proof_revisions"]
        raw = evidence["proof"]
    assert isinstance(raw, bytes) and raw
    return hashlib.sha256(raw).hexdigest()


def main() -> None:
    kind, phase, collection_text, *rest = sys.argv[1:]
    collection_id = int(collection_text)
    store = _store(kind)
    if phase == "prepare":
        assert not rest
        digest = _ingest_until_observed(kind, store, collection_id)
        if kind == "minisign":
            _sign_with_compose_key(store, digest)
        else:
            _mature_with_deterministic_calendar(store, digest)
        evidence_hash = _evidence_hash(kind, store, digest, collection_id)
        print(json.dumps({"digest": digest, "evidence_sha256": evidence_hash}))
    elif phase == "verify":
        assert len(rest) == 2
        digest, expected_hash = rest
        assert _digest_for_collection(collection_id) == digest
        assert _evidence_hash(kind, store, digest, collection_id) == expected_hash
        print("retained")
    else:
        raise ValueError("unknown witness probe phase")


if __name__ == "__main__":
    main()
