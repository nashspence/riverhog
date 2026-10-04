"""Reference-only exact candidate binding; no signer or assertion of human approval.

Production supplies real extracted destination readouts and artifacts, not expected
strings recycled from the compiler. Reuse the existing release manifest/signature.
"""
from __future__ import annotations

from typing import Mapping

from compiler import HEX, SHA, digest, need, path_name, read_json, report_bytes


def candidate_evidence(compiled: dict, *, identity: dict, artifacts: Mapping[str, bytes],
                       observed: Mapping[str, bytes], required_outputs: set[str]) -> bytes:
    """Bind a complete PREPARED candidate before review, not a pre-build approval."""
    need(set(identity) == {"code_sha", "documentation_commit", "version", "compiler_sha256", "toolchain_sha256"},
         "candidate identity fields differ")
    need(all(isinstance(identity[k], str) and SHA.fullmatch(identity[k]) for k in ("code_sha", "documentation_commit"))
         and all(isinstance(identity[k], str) and HEX.fullmatch(identity[k]) for k in ("compiler_sha256", "toolchain_sha256")),
         "exact candidate input identities required")
    need(isinstance(identity["version"], str) and bool(identity["version"]), "candidate version required")
    need(not compiled["coverage"]["missing"], "incomplete coverage is preview, not release evidence")
    need(bool(artifacts) and bool(required_outputs) and set(observed) == required_outputs,
         "candidate lacks exact required destination observations")
    def ledger(values):
        result = {}
        for name, payload in sorted(values.items()):
            path_name(name)
            need(isinstance(payload, bytes), "candidate payload must be captured bytes")
            result[name] = digest(payload)
        return result
    return report_bytes({"format": "riverhog-doc-candidate-reference/v1", "identity": identity,
                         "inputs": compiled["inputs"], "source_files": compiled["source_files"],
                         "compiled_sha256": digest(report_bytes(compiled)),
                         "coverage": compiled["coverage"], "artifacts": ledger(artifacts),
                         "observed": ledger(observed)})


def verify_selected_candidate(selected: bytes, current: bytes, *, artifacts: Mapping[str, bytes],
                              observed: Mapping[str, bytes]) -> None:
    """Integrity check ONLY. The existing protected approval selects/signs the manifest."""
    record = read_json(current)
    need(current == report_bytes(record) and current == selected, "selected candidate changed; review the new candidate")
    for key, payloads in (("artifacts", artifacts), ("observed", observed)):
        need(set(payloads) == set(record[key]) and all(digest(value) == record[key][name]
             for name, value in payloads.items()), "prepared candidate bytes changed after review")
