"""Generate synthetic, deterministic conformance histories; no native OS claims.

Run from this package with the workspace dependencies installed. All times,
namespace properties and transition testimony below are fixture data, not
measurements of a real object store, filesystem or transfer system.
"""

from __future__ import annotations

import io
import itertools
import json
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import riverhog_provenance.common as common
import riverhog_provenance.journal as journals
import riverhog_provenance.observer as engine
import riverhog_provenance.sources as sources
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ObservationRequest,
    SourceEvidence,
    StreamSource,
    append_assertions,
    append_checkpoint,
    append_correction,
    append_observation,
    assertion,
    assertion_reference,
    byte_string,
    create_journal,
    evidence,
    external_reference,
    reference,
    validate_journal,
    validate_journal_set,
)
from riverhog_provenance_contracts import PROFILE, profile_reference

P = Path(__file__).resolve().parents[1] / "examples"
PAYLOAD = b"opaque fixture\x00\xff\x80not a media parser input\n"


class FixtureExposure:
    def __init__(self, payload, context, kind, locator=None, metadata=(), profiles=()):
        self.source = BytesSource(payload)
        self.context, self.kind, self.locator = context, kind, locator
        self.metadata, self.profiles = metadata, profiles

    @contextmanager
    def open(self, *, observer_agent_id):
        with self.source.open(observer_agent_id=observer_agent_id) as session:
            original = session.finalize
            session.capabilities = replace(
                session.capabilities,
                native_metadata=bool(self.metadata or self.profiles),
                persistent_designator=bool(self.locator),
            )
            session.occurrence_kind = self.kind
            session.source_context = self.context
            coverage = tuple(
                {"profile_id": m["profile_id"], "category": m["category"], "status": "complete"}
                for m in self.metadata
            )
            session.finalize = lambda: SourceEvidence(
                consistency=original().consistency,
                address_status="known" if self.locator else "not_exposed",
                locators=({"context_id": self.context["id"], "locator": self.locator},)
                if self.locator
                else (),
                metadata=tuple(self.metadata),
                profiles=tuple(self.profiles),
                coverage=coverage,
            )
            yield session


def save(name, raw):
    summary = validate_journal(raw)
    (P / (name + ".jsonseq")).write_bytes(raw)
    (P / (name + ".materialized.json")).write_text(
        json.dumps(summary.materialize(), indent=2) + "\n"
    )
    return summary


def main():
    P.mkdir(exist_ok=True)
    counter = itertools.count()

    def identity():
        source = "riverhog-provenance-fixture/" + str(next(counter))
        return f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, source)}"

    ticks = itertools.count()

    def now():
        return "2026-09-28T12:00:00." + f"{next(ticks):09d}" + "Z"

    with ExitStack() as stack:
        for module in (common, journals, engine, sources):
            stack.enter_context(patch.object(module, "new_id", identity))
        for module in (common, journals, engine):
            stack.enter_context(patch.object(module, "utc_now", now))
        observer = BoundedSourceObserver()
        first = observer.observe(StreamSource(io.BytesIO(PAYLOAD), expected_length=len(PAYLOAD)))
        who = first.observer_agent_id
        journal = create_journal(first.graph_fragment(), recorded_by_agent_id=who)
        save("never-named-emission", journal)
        (P / "primary.bin").write_bytes(PAYLOAD)

        service = assertion(
            "context", who, kind="object_service", label="Synthetic flat-key service"
        )
        obj = observer.observe(
            FixtureExposure(
                PAYLOAD,
                service,
                "object_store_object",
                locator={
                    "kind": "object_key",
                    "key": {"kind": "text", "text": "literal/slashes/not-paths"},
                    "version": {"kind": "text", "text": "opaque-version-token"},
                },
            ),
            ObservationRequest(artifact=first.artifact),
        )
        journal = append_observation(journal, obj)
        namespace = assertion(
            "context", who, kind="filesystem_namespace", label="Synthetic filesystem namespace"
        )
        native = {
            "profile_id": PROFILE + "/profiles/filesystem",
            "category": PROFILE + "/coverage/extended_attributes",
            "name": {"kind": "text", "text": "user.archival-source"},
            "source": {
                "interface": "urn:fixture:xattr-api",
                "field": {"kind": "text", "text": "user.archival-source"},
                "context_id": namespace["id"],
            },
            "status": "captured",
            "value": {"type": "bytes", "value": byte_string(b"fixture metadata\x00")},
            "sensitivity": "public",
        }
        profile = {
            "profile": profile_reference(PROFILE + "/profiles/filesystem-observation.schema.json"),
            "data": {
                "object_kind": "regular_file",
                "posix_mode": {
                    "octal": "0640",
                    "source": {
                        "interface": "urn:fixture:stat-api",
                        "field": {"kind": "text", "text": "mode"},
                        "context_id": namespace["id"],
                    },
                },
            },
        }
        fs = observer.observe(
            FixtureExposure(
                PAYLOAD,
                namespace,
                "filesystem_object",
                locator={
                    "kind": "filesystem_path",
                    "syntax": "posix",
                    "form": "absolute",
                    "name": {"kind": "text", "text": "/synthetic/item"},
                },
                metadata=(native,),
                profiles=(profile,),
            ),
            ObservationRequest(artifact=first.artifact),
        )
        journal = append_observation(journal, fs)
        changed = observer.observe(StreamSource(io.BytesIO(b"distinct derivative bytes")))
        journal = append_observation(journal, changed)
        activities = []
        relations = []
        for source, result, kind in [
            (first, obj, "materialization"),
            (obj, fs, "copy"),
            (fs, changed, "transformation"),
        ]:
            ev = [
                evidence(
                    who,
                    "attestation",
                    note="Synthetic process fixture; not inferred from payload bytes.",
                )
            ]
            act = assertion(
                "activity",
                who,
                kind=kind,
                associations=[{"agent_id": who, "role": "urn:fixture:workflow"}],
                outcome="success",
                evidence_items=ev,
            )
            use = assertion(
                "usage",
                who,
                activity_id=act["id"],
                state=reference(source.state_id, "state"),
                role="urn:fixture:source",
                evidence_items=ev,
            )
            gen = assertion(
                "generation",
                who,
                activity_id=act["id"],
                state=reference(result.state_id, "state"),
                evidence_items=ev,
            )
            derive = assertion(
                "derivation",
                who,
                used_state=reference(source.state_id, "state"),
                generated_state=reference(result.state_id, "state"),
                kind="transformation" if kind == "transformation" else "copy",
                activity_id=act["id"],
                usage_id=use["id"],
                generation_id=gen["id"],
                evidence_items=ev,
            )
            activities.append(act)
            relations.extend([use, gen, derive])
        journal = append_assertions(
            journal, {"activities": activities, "relations": relations}, recorded_by_agent_id=who
        )
        journal = append_checkpoint(
            journal, recorded_by_agent_id=who, purpose="urn:fixture:handoff"
        )
        mixed = save("mixed-storage-history", journal)
        (P / "derivative.bin").write_bytes(b"distinct derivative bytes")

        # Unknown prehistory is reported, not given invented path, bytes or capture.
        historical = assertion(
            "state",
            who,
            occurrence_id=first.occurrence_id,
            extent={"kind": "unknown", "reason": "Earlier emission boundary not retained"},
        )
        report = assertion(
            "reported_description",
            who,
            state=reference(historical["id"], "state"),
            evidence_items=[
                evidence(
                    who,
                    "attestation",
                    note="An earlier state was reported; no byte evidence survives.",
                )
            ],
        )
        with_report = append_assertions(
            journal, {"states": [historical], "descriptions": [report]}, recorded_by_agent_id=who
        )
        before = validate_journal(with_report)
        replacement = assertion(
            "reported_description",
            who,
            state=reference(historical["id"], "state"),
            evidence_items=[
                evidence(
                    who,
                    "attestation",
                    note="Corrected documentary wording; still no bytes or origin assertion.",
                )
            ],
        )
        with_report = append_correction(
            with_report,
            [assertion_reference(before, report["assertion_id"])],
            reason="Correct documentary wording, not artifact evolution.",
            recorded_by_agent_id=who,
            assertions={"descriptions": [replacement]},
        )
        save("reported-prehistory-and-correction", with_report)

        # Foreign branch is an independent writable journal, not a new suffix on the parent ID.
        fork = observer.observe(BytesSource(b"another derivative"))
        fg = fork.graph_fragment()
        fg["relations"] = [
            assertion(
                "derivation",
                who,
                used_state=external_reference(mixed, fs.state_id),
                generated_state=reference(fork.state_id, "state"),
                kind="transformation",
                evidence_items=[
                    evidence(
                        who, "attestation", note="Synthetic independently recorded derivative."
                    )
                ],
            )
        ]
        foreign = create_journal(fg, recorded_by_agent_id=who, forked_from=mixed.anchor)
        save("independent-journal-fork", foreign)
        validate_journal_set([journal, foreign])

        # Concurrent delivery contexts, neither globally replaces the other.
        contexts = []
        deliveries = []
        for i, result in enumerate((obj, fs)):
            ctx = assertion("context", who, kind="delivery", label=f"Synthetic delivery {i}")
            contexts.append(ctx)
            deliveries.append(
                assertion(
                    "delivery_association",
                    who,
                    delivery_context_id=ctx["id"],
                    slot={"kind": "text", "text": "payload"},
                    role="urn:fixture:payload",
                    state=reference(result.state_id, "state"),
                    verification_observation_id=result.observation_id,
                )
            )
        delivered = append_assertions(
            with_report,
            {"contexts": contexts, "delivery_associations": deliveries},
            recorded_by_agent_id=who,
        )
        save("simultaneous-deliveries", delivered)


if __name__ == "__main__":
    main()
