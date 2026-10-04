"""One reference-only documentation audit record, consumed by CLI, HTML and gating.

The native coordinator must supply complete owned semantics (including applicable
parent/shared/global dependencies), policy and real extracted destination bytes.
A digest pins a baseline; it does not authenticate it or prove that prose is true.
No source discovery, approval service, production renderer or network access here.
"""
from __future__ import annotations

import argparse
import copy
import html
from pathlib import Path
from typing import Mapping

from candidate import candidate_evidence, verify_selected_candidate
from compiler import digest, need, obligations, path_name, read_json, report_bytes, target

SNAPSHOT = "riverhog-doc-impact-snapshot-reference/v1"
AUDIT = "riverhog-documentation-audit-reference/v1"
LEVELS = {"PASS": 0, "REVIEW": 1, "FAIL": 2}


def byte_ledger(values: Mapping[str, bytes]) -> dict[str, str]:
    for name, data in values.items():
        path_name(name)
        need(isinstance(data, bytes), "evidence must be captured bytes")
    return {name: digest(data) for name, data in sorted(values.items())}


def snapshot(compiled: dict, requirements: dict, *, identity: dict,
             semantics: dict, global_semantics: dict, profile: str,
             artifacts: Mapping[str, bytes], expected: Mapping[str, bytes],
             observed: Mapping[str, bytes], destinations: dict[str, list[dict]]) -> dict:
    """Capture source-produced meaning and effective prose, never author-entered hashes."""
    rows = obligations(requirements)
    need(compiled["inputs"]["requirements_sha256"] == digest(report_bytes(requirements))
         and compiled["inputs"]["closure_sha256"] == requirements["closure_sha256"],
         "compiled corpus and source requirements differ")
    need(set(semantics) == set(rows) and isinstance(profile, str) and bool(profile),
         "complete semantic scopes and fingerprint profile required")
    need(isinstance(global_semantics, dict), "global source meaning must be accounted for")
    subjects = {}
    for key, row in sorted(rows.items()):
        scope = semantics[key]
        need(set(scope) == {"owned", "dependencies", "scope"}
             and isinstance(scope["dependencies"], dict) and bool(scope["scope"]),
             "owned semantics need explicit dependencies and scope")
        entry = compiled["resolved"].get(key)
        guides = {}
        for guide in compiled["guides"]:
            if key in {target(item) for item in guide["subjects"]}:
                guides[guide["id"]] = {"title": guide["title"],
                    "markdown": compiled["pages"][guide["body"]]["markdown"]}
        prose = {"entry": None if entry is None else {
            "summary": entry["summary"], "markdown": entry["content"].get("markdown", "")},
            "guides": guides}
        subjects[key] = {"target": row["target"], "authority": row["authority"],
            "interface": row["interface"], "requirement": row, "meaning": scope,
            "prose": prose, "covered": row["rule"] == "structural" or key in compiled["resolved"],
            "location": None if entry is None else {"document_id": entry["document_id"],
                "path": entry["source_path"], "section": entry.get("body")}}
    need(set(expected) == set(destinations) and set(observed) <= set(destinations),
         "source-owned destination inventory and expected/observed evidence differ")
    for name, targets in destinations.items():
        path_name(name)
        keys = [target(item) for item in targets]
        need(bool(keys) and len(keys) == len(set(keys)) and set(keys) <= set(rows),
             "native destinations must map to exact source-owned subjects")
    return copy.deepcopy({"format": SNAPSHOT, "identity": identity,
        "compiled_sha256": digest(report_bytes(compiled)), "inputs": compiled["inputs"],
        "source_files": compiled["source_files"], "policy_sha256": requirements["policy_sha256"],
        "profile": profile, "global_semantics": global_semantics, "subjects": subjects,
        "artifacts": byte_ledger(artifacts), "expected": byte_ledger(expected),
        "observed": byte_ledger(observed), "destinations": destinations})


def checked_snapshot(value: dict) -> None:
    need(isinstance(value, dict) and value.get("format") == SNAPSHOT,
         "unsupported documentation impact snapshot; no empty-diff fallback")
    need(all(key in value for key in ("identity", "compiled_sha256", "inputs", "source_files",
         "policy_sha256", "profile", "global_semantics", "subjects", "artifacts", "expected",
         "observed", "destinations")), "incomplete impact snapshot")
    need(set(value["expected"]) == set(value["destinations"])
         and set(value["observed"]) <= set(value["destinations"]), "incomplete destination inventory")
    for key, row in value["subjects"].items():
        need(target(row["target"]) == key, "snapshot target differs from stable identity")


def build_audit(current: dict, *, stage: str, baseline: dict | None = None,
                baseline_sha256: str | None = None, initial: bool = False) -> dict:
    """Compute once. REVIEW is attention, not a finding that prose is false."""
    checked_snapshot(current)
    need(stage in {"preview", "prepared"}, "unknown audit stage")
    if baseline is None:
        need(initial and baseline_sha256 is None,
             "baseline unavailable; explicitly select initial review only when no baseline exists")
    else:
        checked_snapshot(baseline)
        need(not initial and baseline_sha256 == digest(report_bytes(baseline)),
             "selected baseline identity differs")
    findings = []
    def add(level, code, key, detail):
        findings.append({"id": digest(report_bytes([code, key]))[:20], "state": level,
                         "code": code, "subject": key, "detail": detail})
    if baseline is None:
        add("REVIEW", "initial-review", None, "No prior baseline: full initial review, not no drift.")
    comparable = baseline is not None and baseline["profile"] == current["profile"]
    if baseline is not None and not comparable:
        add("REVIEW", "incomparable-meaning-profile", None,
            "Fingerprint rules changed: meaning equality is unknown; inspect full contract delta.")
    if baseline is not None and baseline["policy_sha256"] != current["policy_sha256"]:
        add("REVIEW", "requirement-policy-changed", None, "Inspect requirement classification changes.")
    if baseline is not None and baseline["global_semantics"] != current["global_semantics"]:
        add("REVIEW", "global-meaning-changed", None,
            "Global/unattributed meaning changed; local unchanged fingerprints are not complete assurance.")
    old = {} if baseline is None else baseline["subjects"]
    deltas = []
    for key in sorted(set(old) | set(current["subjects"])):
        before, after = old.get(key), current["subjects"].get(key)
        if after is not None and not after["covered"]:
            add("FAIL", "missing-documentation", key, "Required subject or selected detail is missing.")
        if baseline is None:
            change = "initial"
        elif before is None:
            change = "added"
            add("REVIEW", "subject-added", key, "New source-derived documentation obligation.")
        elif after is None:
            change = "removed"
            add("REVIEW", "subject-removed", key, "Obligation removed; verify no meaning was lost.")
        else:
            meaning = None if not comparable else before["meaning"] != after["meaning"]
            prose = before["prose"] != after["prose"]
            change = ("unknown-meaning" if meaning is None else
                      "meaning-and-prose" if meaning and prose else
                      "meaning-only" if meaning else "prose-only" if prose else "unchanged")
            if change != "unchanged":
                add("REVIEW", change, key, "Inspect before/after meaning and effective prose in context.")
            if before["requirement"] != after["requirement"]:
                add("REVIEW", "requirement-changed", key,
                    "Rule/detail/canonical target/ownership changed, including relaxations.")
        fingerprints = {}
        for label, row in (("before", before), ("after", after)):
            fingerprints[label] = None if row is None else {
                "meaning_sha256": digest(report_bytes(row["meaning"])),
                "effective_prose_sha256": digest(report_bytes(row["prose"])),
                "requirement_sha256": digest(report_bytes(row["requirement"]))}
        deltas.append({"subject": key, "change": change, "before": before, "after": after,
                       "fingerprints": fingerprints})
    outputs = []
    if not current["destinations"] or (stage == "prepared" and not current["artifacts"]):
        add("FAIL" if stage == "prepared" else "REVIEW", "native-evidence-unavailable", None,
            "No complete native output or prepared artifact set was supplied.")
    for name in sorted(current["destinations"]):
        actual, wanted = current["observed"].get(name), current["expected"][name]
        state = "unverified" if actual is None else "matches" if actual == wanted else "mismatch"
        outputs.append({"name": name, "status": state, "expected": wanted, "observed": actual,
                        "subjects": current["destinations"][name]})
        if state != "matches":
            for t in current["destinations"][name]:
                add("REVIEW" if state == "unverified" and stage == "preview" else "FAIL",
                    "native-" + state + ":" + name, target(t),
                    "Native bytes not extracted yet." if state == "unverified" else
                    "Extracted native bytes differ from the identified expected projection.")
        if baseline is not None and baseline["expected"].get(name) != wanted:
            add("REVIEW", "destination-projection-changed:" + name, None,
                "Expected destination changed; inspect output even if prose stayed the same.")
    if baseline is not None:
        for name in sorted(set(baseline["destinations"]) - set(current["destinations"])):
            add("REVIEW", "destination-removed:" + name, None,
                "Previously required native destination removed; inspect adapter/policy disposition.")
    findings.sort(key=lambda x: (-LEVELS[x["state"]], x["code"], x["subject"] or ""))
    state = max((x["state"] for x in findings), key=LEVELS.get, default="PASS")
    return {"format": AUDIT, "stage": stage, "state": state,
        "current": current, "current_sha256": digest(report_bytes(current)),
        "baseline": None if baseline is None else {"sha256": baseline_sha256,
            "identity": baseline["identity"], "profile": baseline["profile"]},
        "comparison": "initial" if baseline is None else "comparable" if comparable else "incomparable",
        "findings": findings, "deltas": deltas, "outputs": outputs}


def validate_record(record: dict) -> None:
    need(record.get("format") == AUDIT and record.get("stage") in {"preview", "prepared"},
         "unsupported documentation audit record")
    checked_snapshot(record["current"])
    need(record["current_sha256"] == digest(report_bytes(record["current"])), "audit input identity differs")
    need(all(f["state"] in LEVELS for f in record["findings"]), "unknown audit finding state")
    actual = max((f["state"] for f in record["findings"]), key=LEVELS.get, default="PASS")
    need(record["state"] == actual, "audit summary suppresses findings")
    mechanical = build_audit(record["current"], stage=record["stage"], initial=True)
    blockers = {(f["code"], f["subject"]) for f in mechanical["findings"] if f["state"] == "FAIL"}
    retained = {(f["code"], f["subject"]) for f in record["findings"] if f["state"] == "FAIL"}
    need(blockers <= retained and record["outputs"] == mechanical["outputs"],
         "audit hides mechanical blockers or changes destination observations")


def check_record(record: dict, *, prepared: bool = False) -> int:
    """Integrity/coverage gate, not human approval. A REVIEW is not an automatic veto."""
    validate_record(record)
    if prepared and record["stage"] != "prepared":
        return 2
    return 2 if record["state"] == "FAIL" else 0


def terminal_summary(record: dict, *, limit: int = 12) -> str:
    validate_record(record)
    need(limit >= 0, "negative summary limit")
    lines = [f"Documentation Audit: {record['state']} ({record['stage']}; {record['comparison']})",
             "PASS concerns performed checks only; no state certifies prose truth or human approval."]
    lines.extend(f"{f['state']} {f['code']}: {f['subject'] or 'whole candidate'}"
                 for f in record["findings"][:limit])
    omitted = len(record["findings"]) - limit
    if omitted > 0:
        lines.append(f"{omitted} more findings; full record and HTML retain every finding.")
    return "\n".join(lines) + "\n"


def render_panel(record: dict, *, subject: str | None = None) -> str:
    """Escaped composable HTML fragment. Native shared renderer owns routes/mode controls."""
    validate_record(record)
    esc = lambda x: html.escape(str(x), quote=True)
    findings = [f for f in record["findings"] if subject is None or f["subject"] in {None, subject}]
    parts = [f'<section aria-label="Documentation Audit"><h2>Documentation Audit: {esc(record["state"])}</h2>',
             f'<p>{esc(record["stage"])} / {esc(record["comparison"])}</p>',
             '<p>Mechanical evidence and review attention, not prose certification or human approval.</p>']
    for f in findings:
        parts.append(f'<p id="doc-audit-{f["id"]}"><strong>{esc(f["state"])} {esc(f["code"])}</strong>: '
                     f'{esc(f["detail"])} <code>{esc(f["subject"] or "whole candidate")}</code></p>')
    for d in record["deltas"]:
        if subject is not None and d["subject"] != subject:
            continue
        parts.append(f'<details><summary>{esc(d["change"])}: {esc(d["subject"])}</summary>')
        for label in ("before", "after"):
            parts.append(f'<h3>{label.title()}</h3><pre>{esc(report_bytes(d[label]).decode())}</pre>')
        parts.append('</details>')
    for output in record["outputs"]:
        if subject is None or subject in {target(t) for t in output["subjects"]}:
            parts.append(f'<p>Destination <code>{esc(output["name"])}</code>: {esc(output["status"])}</p>')
    return "".join(parts) + '</section>'


def audited_candidate_evidence(compiled: dict, *, identity: dict, artifacts: Mapping[str, bytes],
        observed: Mapping[str, bytes], required_outputs: set[str], audit: dict) -> bytes:
    """Bind audit before rendering review evidence; no self-hash or second approval."""
    need(check_record(audit, prepared=True) == 0, "prepared documentation audit blocks candidate")
    current = audit["current"]
    need(current["identity"] == identity
         and current["compiled_sha256"] == digest(report_bytes(compiled))
         and current["artifacts"] == byte_ledger(artifacts)
         and current["observed"] == byte_ledger(observed)
         and set(current["destinations"]) == required_outputs, "audit belongs to another prepared candidate")
    result = read_json(candidate_evidence(compiled, identity=identity, artifacts=artifacts,
                      observed=observed, required_outputs=required_outputs))
    result["documentation_audit_sha256"] = digest(report_bytes(audit))
    result["documentation_audit_panel_sha256"] = digest(render_panel(audit).encode())
    return report_bytes(result)


def verify_audited_candidate(selected: bytes, current: bytes, *, artifacts: Mapping[str, bytes],
        observed: Mapping[str, bytes], audit: bytes, panel: bytes) -> None:
    verify_selected_candidate(selected, current, artifacts=artifacts, observed=observed)
    record = read_json(current)
    need(record["documentation_audit_sha256"] == digest(audit)
         and record["documentation_audit_panel_sha256"] == digest(panel),
         "selected audit or review panel changed")
    parsed = read_json(audit)
    need(check_record(parsed, prepared=True) == 0 and panel == render_panel(parsed).encode(),
         "review panel does not express the selected prepared audit")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("record", type=Path, help="Generated audit JSON; never an author input.")
    p.add_argument("--html", type=Path)
    p.add_argument("--prepared", action="store_true")
    args = p.parse_args()
    record = read_json(args.record.read_bytes())
    print(terminal_summary(record), end="")
    if args.html:
        args.html.write_text(render_panel(record), encoding="utf-8")
    return check_record(record, prepared=args.prepared)


if __name__ == "__main__":
    raise SystemExit(main())
