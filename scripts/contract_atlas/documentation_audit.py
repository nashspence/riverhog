"""One nonnormative documentation record for coverage, native parity and contextual drift."""

from __future__ import annotations

import copy
from collections import Counter, defaultdict
from collections.abc import Mapping
from typing import Any

from .documentation_markdown import selected_content
from .documentation_requirements import target_key
from .model import ContractAtlasError, canonical_sha256

SNAPSHOT_FORMAT = "riverhog-documentation-impact-snapshot/v1"
AUDIT_FORMAT = "riverhog-documentation-audit/v1"
AUDIT_FILENAME = "documentation-audit.json"
LEVELS = {"PASS": 0, "REVIEW": 1, "FAIL": 2}


def build_snapshot(
    closure: Mapping[str, Any],
    compiled: Mapping[str, Any],
    requirements: Mapping[str, Any],
    *,
    identity: Mapping[str, Any],
    expected: Mapping[str, Any] | None = None,
    observed: Mapping[str, Any] | None = None,
    artifacts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Retain complete scopes and raw readouts once, outside the human summary budget."""
    if compiled["inputs"]["requirements_sha256"] != canonical_sha256(requirements) or requirements[
        "closure_sha256"
    ] != canonical_sha256(closure):
        raise ContractAtlasError("documentation and requirements belong to different semantics")
    missing = {target_key(item) for item in compiled["coverage"]["missing"]}
    guides = {
        guide["id"]: {
            "title": guide["title"],
            "markdown": compiled["pages"][guide["body"]]["markdown"],
            "subjects": sorted(guide["subjects"], key=canonical_sha256),
        }
        for guide in compiled["guides"]
    }
    guide_bindings: dict[str, dict[str, str]] = defaultdict(dict)
    for name, guide in guides.items():
        digest = canonical_sha256(guide)
        for target in guide["subjects"]:
            guide_bindings[target_key(target)][name] = digest
    subjects = {}
    for key, row in sorted(requirements["subjects"].items()):
        entry = compiled["resolved"].get(key)
        subjects[key] = {
            "target": row["target"],
            "authority": row["authority"],
            "interface": row["interface"],
            "title": row["title"],
            "requirement": {
                k: v
                for k, v in row.items()
                if k not in {"meaning", "target", "authority", "interface", "title"}
            },
            "meaning": row["meaning"],
            "covered": key not in missing,
            "prose": {
                "entry": None
                if entry is None
                else {
                    "summary": entry["summary"],
                    "markdown": selected_content(compiled, entry).get("markdown", ""),
                },
                "guides": guide_bindings.get(key, {}),
                "canonical": row.get("canonical"),
            },
            "location": None
            if entry is None
            else {
                "document_id": entry["document_id"],
                "path": entry["source_path"],
                "section": entry.get("body"),
            },
        }
    return copy.deepcopy(
        {
            "format": SNAPSHOT_FORMAT,
            "identity": identity,
            "closure": closure,
            "compiled_sha256": canonical_sha256(compiled),
            "requirements_sha256": canonical_sha256(requirements),
            "policy_sha256": requirements["policy_sha256"],
            "policy": requirements["policy"],
            "profile": requirements["fingerprint_profile"],
            "source_files": compiled["source_files"],
            "guides": guides,
            "scopes": requirements["scopes"],
            "contexts": requirements["contexts"],
            "subjects": subjects,
            "global_semantics": requirements["global_semantics"],
            "expected": expected or {},
            "observed": observed or {},
            "artifacts": artifacts or {},
        }
    )


def _snapshot(value: Mapping[str, Any]) -> None:
    if value.get("format") != SNAPSHOT_FORMAT or not {
        "identity",
        "closure",
        "compiled_sha256",
        "requirements_sha256",
        "policy_sha256",
        "policy",
        "profile",
        "source_files",
        "guides",
        "scopes",
        "contexts",
        "subjects",
        "global_semantics",
        "expected",
        "observed",
        "artifacts",
    } <= set(value):
        raise ContractAtlasError("unsupported or incomplete documentation impact snapshot")
    if canonical_sha256(value["policy"]) != value["policy_sha256"]:
        raise ContractAtlasError("documentation snapshot policy identity differs")
    for key, row in value["subjects"].items():
        if target_key(row["target"]) != key:
            raise ContractAtlasError("documentation snapshot changed a stable target")
        context_id = row["meaning"]["dependencies"]
        if context_id not in value["contexts"]:
            raise ContractAtlasError("documentation snapshot lacks its semantic context")
        scope_ids = {row["meaning"]["owned"], *value["contexts"][context_id]}
        if not scope_ids <= set(value["scopes"]):
            raise ContractAtlasError("documentation snapshot lacks its owned semantic context")
        if any(
            name not in value["guides"] or canonical_sha256(value["guides"][name]) != digest
            for name, digest in row["prose"]["guides"].items()
        ):
            raise ContractAtlasError("documentation snapshot changed its selected guidance")
    if any(canonical_sha256(scope) != identity for identity, scope in value["scopes"].items()):
        raise ContractAtlasError("documentation semantic scope identity differs")
    if any(
        canonical_sha256(context) != identity for identity, context in value["contexts"].items()
    ):
        raise ContractAtlasError("documentation semantic context identity differs")
    if not set(value["observed"]) <= set(value["expected"]):
        raise ContractAtlasError(
            "documentation has observations outside native destination ownership"
        )


def build_audit(
    current: Mapping[str, Any],
    *,
    stage: str,
    baseline: Mapping[str, Any] | None = None,
    baseline_custody: Mapping[str, Any] | None = None,
    initial: bool = False,
) -> dict[str, Any]:
    """Compute attention once; renderers and command exit codes consume this exact record."""
    _snapshot(current)
    if stage not in {"preview", "prepared"}:
        raise ContractAtlasError("unknown documentation audit stage")
    if baseline is None:
        if not initial or baseline_custody is not None:
            raise ContractAtlasError("baseline unavailable; explicit initial review is required")
    else:
        _snapshot(baseline)
        if (
            initial
            or baseline_custody is None
            or baseline_custody.get("snapshot_sha256") != canonical_sha256(baseline)
            or baseline_custody.get("kind")
            not in {"published-product", "selected-prepared-candidate"}
            or not baseline_custody.get("manifest_sha256")
            or not baseline_custody.get("source_sha")
            or not baseline_custody.get("verification")
        ):
            raise ContractAtlasError(
                "selected documentation baseline lacks verified candidate custody"
            )
    findings: list[dict[str, Any]] = []

    def add(state: str, code: str, subject: str | None, detail: str) -> None:
        findings.append(
            {
                "id": canonical_sha256([code, subject])[:24],
                "state": state,
                "code": code,
                "subject": subject,
                "detail": detail,
            }
        )

    if baseline is None:
        add(
            "REVIEW",
            "initial-review",
            None,
            "No prior product baseline; review the full current corpus and contract.",
        )
    comparable = baseline is not None and baseline["profile"] == current["profile"]
    supported = (
        isinstance(current["profile"], dict)
        and current["profile"].get("format") == "riverhog-documentation-owned-context/v1"
    )
    comparable = comparable and supported
    if baseline is not None and not comparable:
        add(
            "REVIEW",
            "incomparable-meaning-profile",
            None,
            (
                "Meaning comparison is unknown under different fingerpri"
                "nt policies; inspect full retained values."
            ),
        )
    if baseline is not None and baseline["policy_sha256"] != current["policy_sha256"]:
        add(
            "REVIEW",
            "requirement-policy-changed",
            None,
            "Inspect native classification, ownership and requirement deltas.",
        )
    if baseline is not None and baseline["global_semantics"] != current["global_semantics"]:
        add(
            "REVIEW",
            "global-meaning-changed",
            None,
            "Global or unattributed meaning changed; local scope equality is insufficient.",
        )
    if baseline is not None and baseline["identity"].get("source_sha") != current["identity"].get(
        "source_sha"
    ):
        add(
            "REVIEW",
            "implementation-context-changed",
            None,
            (
                "Code revision changed. Native scoped values do not esta"
                "blish unchanged implementation behavior; inspect the se"
                "lected source revisions."
            ),
        )

    old = {} if baseline is None else baseline["subjects"]
    deltas = []
    for key in sorted(set(old) | set(current["subjects"])):
        before, after = old.get(key), current["subjects"].get(key)
        if after is not None and not after["covered"]:
            add(
                "FAIL",
                "missing-documentation",
                key,
                "Mandatory subject explanation or selected detail is missing.",
            )
        if baseline is None:
            change = "initial"
        elif before is None:
            change = "added"
            add("REVIEW", "subject-added", key, "New native documentation obligation.")
        elif after is None:
            change = "removed"
            add(
                "REVIEW",
                "subject-removed",
                key,
                "Removed obligation remains visible; inspect the former meaning and disposition.",
            )
        else:
            meaning = None if not comparable else before["meaning"] != after["meaning"]
            prose = before["prose"] != after["prose"]
            change = (
                "unknown-meaning"
                if meaning is None
                else "meaning-and-prose"
                if meaning and prose
                else "meaning-only"
                if meaning
                else "prose-only"
                if prose
                else "unchanged"
            )
            if change != "unchanged":
                add(
                    "REVIEW",
                    change,
                    key,
                    "Inspect owned values, applicable context and selected prose before and after.",
                )
            if before["requirement"] != after["requirement"]:
                add(
                    "REVIEW",
                    "requirement-changed",
                    key,
                    (
                        "Requirement, detail, destination, ownership or canonica"
                        "l donor changed; relaxations require review."
                    ),
                )
        deltas.append(
            {
                "subject": key,
                "change": change,
                "before": True if before is not None else None,
                "after": True if after is not None else None,
            }
        )

    outputs = []
    for name in sorted(current["expected"]):
        wanted, actual = current["expected"][name], current["observed"].get(name)
        status = "unverified" if actual is None else "matches" if actual == wanted else "mismatch"
        outputs.append({"name": name, "status": status, "expected": wanted, "observed": actual})
        if actual is None:
            add(
                "FAIL" if stage == "prepared" else "REVIEW",
                "native-unverified",
                name,
                "Actual prepared artifact extraction was not performed for this destination.",
            )
        elif actual != wanted:
            add(
                "FAIL",
                "native-mismatch",
                name,
                "Actual prepared destination differs from its selected projection.",
            )
        if baseline is not None and baseline["expected"].get(name) != wanted:
            add(
                "REVIEW",
                "native-destination-changed",
                name,
                "Expected destination content or membership changed.",
            )
    if baseline is not None:
        for name in sorted(set(baseline["expected"]) - set(current["expected"])):
            add(
                "REVIEW",
                "native-destination-removed",
                name,
                "The retained baseline had a destination absent from this candidate.",
            )
    if not current["expected"] or (stage == "prepared" and not current["artifacts"]):
        add(
            "FAIL" if stage == "prepared" else "REVIEW",
            "native-evidence-unavailable",
            None,
            (
                "Complete actual destination readouts and prepared artif"
                "act identities are not established."
            ),
        )

    # Reuse the native owned/global value comparator. No renderer computes its own drift.
    from .review import _changes

    contract_delta = (
        []
        if baseline is None
        else _changes(
            {k: v for k, v in baseline["closure"].items() if k != "elements"},
            {k: v for k, v in current["closure"].items() if k != "elements"},
        )
    )
    owned = {p for item in current["closure"]["elements"] for p in item["pointers"]}
    if baseline is not None:
        owned.update(p for item in baseline["closure"]["elements"] for p in item["pointers"])
    for delta in contract_delta:
        delta["unattributed"] = not any(
            delta["pointer"] == p or delta["pointer"].startswith(p + "/") for p in owned
        )
    if any(delta["unattributed"] for delta in contract_delta):
        add(
            "REVIEW",
            "unattributed-meaning-changed",
            None,
            "Native global comparison contains changes outside owned subjects.",
        )
    old_guides = {} if baseline is None else baseline["guides"]
    guide_deltas = []
    for name in sorted(set(old_guides) | set(current["guides"])):
        before, after = old_guides.get(name), current["guides"].get(name)
        change = (
            "initial"
            if baseline is None
            else "added"
            if before is None
            else "removed"
            if after is None
            else "changed"
            if before != after
            else "unchanged"
        )
        if baseline is not None and change != "unchanged":
            add(
                "REVIEW",
                "guide-" + change,
                "guide/" + name,
                "Inspect retained guidance and declared subjects before and after: " + name,
            )
        guide_deltas.append(
            {
                "guide_id": name,
                "change": change,
                "before": True if before is not None else None,
                "after": True if after is not None else None,
            }
        )
    state = max((f["state"] for f in findings), key=LEVELS.__getitem__, default="PASS")
    return {
        "format": AUDIT_FORMAT,
        "stage": stage,
        "state": state,
        "current": copy.deepcopy(current),
        "baseline": copy.deepcopy(baseline),
        "baseline_custody": copy.deepcopy(baseline_custody),
        "comparison": "initial-review"
        if baseline is None
        else "comparable"
        if comparable
        else "incomparable",
        "checks": {
            "coverage": "performed",
            "native": "actual-extraction" if current["observed"] else "not-performed",
            "impact": "initial-review" if baseline is None else "performed",
        },
        "findings": findings,
        "deltas": deltas,
        "guide_deltas": guide_deltas,
        "contract_delta": contract_delta,
        "policy_delta": [] if baseline is None else _changes(baseline["policy"], current["policy"]),
        "destinations": outputs,
        "counts": {
            "subjects": len(current["subjects"]),
            "covered": sum(s["covered"] for s in current["subjects"].values()),
            "findings": dict(Counter(f["state"] for f in findings)),
            "changes": dict(Counter(d["change"] for d in deltas)),
        },
    }


def check_record(record: Mapping[str, Any], *, prepared: bool = False) -> int:
    if record.get("format") != AUDIT_FORMAT or record.get("stage") not in {"preview", "prepared"}:
        raise ContractAtlasError("unsupported documentation audit record")
    rebuilt = build_audit(
        record["current"],
        stage=record["stage"],
        baseline=record["baseline"],
        baseline_custody=record["baseline_custody"],
        initial=record["baseline"] is None,
    )
    if rebuilt != record:
        raise ContractAtlasError(
            "documentation audit findings or aggregate state differ from their evidence"
        )
    return 2 if record["state"] == "FAIL" or (prepared and record["stage"] != "prepared") else 0


def terminal_summary(record: Mapping[str, Any], *, limit: int = 30) -> str:
    check_record(record)
    current = record["current"]
    lines = [
        f"Documentation {record['state']} ({record['stage']}); "
        f"{record['counts']['covered']}/{record['counts']['subjects']} subjects covered.",
        f"Comparison: {record['comparison']}; native extraction: {record['checks']['native']}.",
    ]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for finding in record["findings"]:
        subject = current["subjects"].get(finding["subject"])
        group = (
            (subject["authority"], subject["interface"]) if subject else ("candidate", "evidence")
        )
        grouped.setdefault(group, []).append(finding)
    remaining = limit
    for (authority, interface), findings in sorted(grouped.items()):
        lines.append(f"{authority} / {interface}: {len(findings)} findings")
        for finding in findings[: max(0, remaining)]:
            subject = current["subjects"].get(finding["subject"])
            lines.append(
                f"  {finding['state']} {finding['code']}: "
                f"{subject['title'] if subject else finding['detail']}"
            )
            if subject is not None:
                lines.append("    target: " + target_key(subject["target"]))
            remaining -= 1
    lines.append(
        f"Full evidence: {AUDIT_FILENAME}. "
        "Presence and parity do not establish prose truth or human approval."
    )
    return "\n".join(lines) + "\n"
