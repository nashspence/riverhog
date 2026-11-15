"""Offline-only extraction of the published Native phase/control receipt.

Uses the standard library and read-only git commands. It never imports product
modules, constructs fixtures, opens a database, or executes a validation method.
The original raw receipt remains authoritative; selected pairs are copied as
parsed JSON records without rewriting identities or checkpoint hashes.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path
import subprocess
import sys


RAW_SHA = "019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b"
SOURCE = "d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e"
REFERENCE = "8b639901c4dbe157dce904acec718a23f580c6ca"
NATIVE = "packages/riverhog-provenance/src/riverhog_provenance/"
CORE = "riverhog/src/riverhog_core/services/"
PINS = (
    "AGENTS.md", "README.md", "mise.toml", "uv.lock", "scripts/extent_contract.py",
    *(NATIVE + name + ".py" for name in (
        "_semantic_finish", "_entry_replay", "_generic_semantics", "_session_checkpoint",
        "journal_session", "journal_store", "_session_input", "_journal_prefix",
        "_entry_preparation", "_assertion_staging", "_correction_targets",
        "_correction_projection", "_online_dag", "_semantic_program",
    )),
    *(CORE + name + ".py" for name in (
        "collection_uploads", "upload_journal_rank", "upload_journal_validation_owner",
        "upload_journal_validation_jobs", "upload_journal_certificates",
    )),
    *("packages/riverhog-provenance/tests/" + name + ".py" for name in (
        "test_semantic_finish", "test_validation_checkpoint", "test_entry_replay",
        "test_retained_journal_session", "test_entry_replay_child_growth",
        "test_entry_replay_advance_growth", "test_journal_session_growth",
    )),
)
CHILDREN = (
    "EntryPreparation", "AssertionStaging", "GenericSemantics", "SemanticFinish",
    "CorrectionTargets", "CorrectionWork",
)


def receipt(data: bytes) -> dict[str, int | str]:
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def add_counts(records: list[dict], field: str) -> dict[str, int]:
    total: Counter = Counter()
    for record in records:
        total.update(record[field])
    return dict(sorted(total.items()))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--source", default=SOURCE)
    parser.add_argument("--historical-reference", default=REFERENCE)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert args.source == SOURCE and args.historical_reference == REFERENCE
    repo = args.repo.resolve()

    def git(*words: str) -> bytes:
        return subprocess.check_output(["git", *words], cwd=repo)

    def published(path: str) -> bytes:
        return git("show", args.historical_reference + ":.reference/" + path)

    raw = args.raw.read_bytes()
    assert receipt(raw)["sha256"] == RAW_SHA
    compressed = published("evidence/observation/run.json.gz")
    assert gzip.decompress(compressed) == raw
    data = json.loads(raw)
    old = data["source_start"]["source_sha"]
    assert data["source_start"] == data["source_end"] == {
        "source_clean": True, "source_sha": "3b47cbba770c31eb1487b50f7e3b16533c10e827"
    }
    assert git("rev-parse", REFERENCE + "^").decode().strip() == old
    expected_delta = [
        NATIVE + "_session_checkpoint.py",
        "packages/riverhog-provenance/tests/test_validation_checkpoint.py",
    ]
    delta = git("diff", "--name-only", old, SOURCE).decode().splitlines()
    assert delta == expected_delta
    assert not git("diff", "--name-only", SOURCE, "--", ".", ":(exclude).reference")
    assert data["status"] == "complete" and data["intended_exit_code"] == 0
    assert data["budget"] == {"rows": 64, "octets": 33619968, "source_octets": 65536}
    assert not data["cooperative_cap_reached"]
    observed = data["observation"]
    assert len(observed["hooks"]) == 71 and observed["hook_identities_restored"] is True
    assert data["ordinary_resources"]["retained_original_handles"] == 20
    assert all(data["ordinary_resources"]["storage_received"])
    assert not data["ordinary_resources"]["cleanup_errors"]
    assert data["temporary"]["original_resources_received"]
    assert data["temporary"]["owner_removed_workspace"]
    turns = observed["advances"]
    assert len(turns) == 479 and [x["ordinal"] for x in turns] == list(range(479))

    original_receipts = {}
    for name in ("execution", "lane"):
        payload = published("evidence/observation/" + name + ".json")
        original_receipts[name] = {**receipt(payload), "value": json.loads(payload)}
    execution = original_receipts["execution"]["value"]
    lane = original_receipts["lane"]["value"]
    assert execution["original_child_exit_status"] == execution["launcher_exit_status"] == 0
    assert lane["exit_status"] == 0 and execution["cleanup"]["temporary_removed"]

    source_pins = {}
    for path in PINS:
        blob = git("show", SOURCE + ":" + path)
        assert (repo / path).read_bytes() == blob
        source_pins[path] = {
            **receipt(blob), "git_blob": git("rev-parse", SOURCE + ":" + path).decode().strip(),
            "same_bytes_as_historical_source": git("show", old + ":" + path) == blob,
        }
    inputs = []
    for item in data["inputs"]:
        for suffix, digest_key in ((".jsonseq", "sha256"), ("-expected.json", "expected_sha256")):
            name = item["name"] + suffix
            payload = published("evidence/observation/" + name)
            assert receipt(payload)["sha256"] == item[digest_key]
            local = args.raw.parent / name
            assert local.read_bytes() == payload
            inputs.append({"path_in_reference": "evidence/observation/" + name, **receipt(payload)})

    histories = []
    pairs = []
    for name, count in (("initial", 107), ("checkpoint", 125), ("correction", 247)):
        selected = [x for x in turns if x["case"] == name]
        assert len(selected) == count
        assert [x["before"]["version"] for x in selected] == list(range(count))
        assert [x["returned"]["version"] for x in selected] == list(range(1, count + 1))
        assert all(x["budget"] == data["budget"] for x in selected)
        assert all(x["method_calls"]["ValidationLedger._fence"] == 1 for x in selected)
        assert all(x["method_calls"]["ValidationLedger._publish"] == 1 for x in selected)
        methods = add_counts(selected, "method_calls")
        admissions = sum(methods.get(owner + ".prepare_admission", 0) for owner in CHILDREN)
        preparations = sum(methods.get(owner + ".prepare", 0) for owner in CHILDREN)
        completions = methods["EntryReplay._child_step"] - admissions - preparations
        assert completions == admissions
        per_frame = []
        frames = next(x["frames"] for x in data["inputs"] if x["name"] == name)
        for frame in range(frames):
            frame_turns = [x for x in selected if x["before"]["frame_ordinal"] == frame]
            transitions: Counter = Counter()
            for turn in frame_turns:
                for child in turn["child_returns"]:
                    if child["operation"].endswith(".prepare") and "before_json" in child:
                        before = json.loads(child["before_json"])
                        application = child["operation"].removesuffix(".prepare") + "._apply"
                        applied = [x for x in turn["child_returns"] if x["operation"] == application]
                        assert len(applied) == 1 and isinstance(applied[0]["state"], dict)
                        after = applied[0]["state"]
                        if child["after_state"] is not None:
                            assert child["after_state"] == after
                        transitions[(child["operation"], turn["before"]["phase"],
                                     before.get("phase"), after.get("phase"))] += 1
            per_frame.append({
                "frame_ordinal": frame,
                "frame_kind": next(x for x in data["inputs"] if x["name"] == name)["frame_kinds"][frame],
                "replay_calls_including_frame_admission": sum(
                    x["method_calls"].get("EntryReplay.advance", 0)
                    + x["method_calls"].get("EntryReplay.admit", 0) for x in frame_turns
                ),
                "child_prepare_transitions": [
                    {"operation": k[0], "parent_phase": k[1], "before_phase": k[2],
                     "after_phase": k[3], "after_basis": "actual matching _apply.state", "calls": v}
                    for k, v in sorted(transitions.items())
                ],
            })
        for index, turn in enumerate(selected):
            final = [x for x in turn["child_returns"] if x["operation"] == "SemanticFinish.prepare"
                     and x["after_state"]["phase"] == "done"]
            if not final:
                continue
            assert len(final) == 1
            promotion = selected[index + 1]
            done = final[0]["after_state"]
            applied = [x for x in turn["child_returns"] if x["operation"] == "SemanticFinish._apply"]
            assert len(applied) == 1 and applied[0]["state"] == done
            assert promotion["before"]["child_id"] == turn["before"]["child_id"]
            assert promotion["before"]["phase"] == "finish"
            assert turn["returned"]["phase"] == "finish"
            assert promotion["returned"]["phase"] == "recording"
            assert len(promotion["child_returns"]) == 1
            assert promotion["child_returns"][0]["operation"] == "SemanticFinish.load"
            assert promotion["child_returns"][0]["state"] == done
            assert turn["dbapi_calls"]["commit"] == promotion["dbapi_calls"]["commit"] == 1
            pairs.append({"case": name, "frame_ordinal": turn["before"]["frame_ordinal"],
                          "final_finish_turn": turn, "parent_promotion_turn": promotion})
        histories.append({
            "name": name, "input": next(x for x in data["inputs"] if x["name"] == name),
            "retained_advances": count, "method_counts": methods,
            "dbapi_counts_within_advances": add_counts(selected, "dbapi_calls"),
            "child_step_partition": {"admissions": admissions, "preparations": preparations,
                                     "completion_recognitions": completions,
                                     "total": methods["EntryReplay._child_step"]},
            "single_start_turns": sum(len(x["starts"]) == 1 for x in selected),
            "double_start_turns": sum(len(x["starts"]) == 2 for x in selected),
            "per_frame": per_frame,
            "original_case_receipt": next(x for x in data["cases"] if x["name"] == name),
        })
    assert [(p["final_finish_turn"]["ordinal"], p["parent_promotion_turn"]["ordinal"])
            for p in pairs] == [(100, 101), (207, 208), (225, 226), (332, 333), (472, 473)]
    promotions = [p["parent_promotion_turn"] for p in pairs]
    total_methods = add_counts(turns, "method_calls")
    assert total_methods["SemanticFinish.prepare"] == 101
    assert total_methods["SemanticFinish.load"] == 106
    assert total_methods["ValidationLedger._publish"] == 479
    assert add_counts(turns, "dbapi_calls")["commit"] == 482
    assert add_counts(promotions, "dbapi_calls") == {"commit": 5, "execute": 67, "fetchone": 57}

    report_bytes = args.report.read_bytes()
    script_path = Path(__file__).resolve()
    result = {
        "format": "non-authoritative-native-granularity-static-inventory/v1",
        "analysis": {"script": str(script_path), **receipt(script_path.read_bytes()),
                     "command": [sys.executable, "-B", str(script_path), *sys.argv[1:]],
                     "python": sys.version, "product_imports": 0, "product_calls": 0,
                     "tests_run": 0, "new_observations": 0,
                     "method": "stdlib JSON/gzip/hash processing and read-only git; no product code",
                     "earlier_offline_attempt": {
                         "original_exit_status": 1,
                         "script_sha256": "44203920c35a5d0c4312af825f54e7b6e07f7d86c0204a93f470f9697274b211",
                         "product_calls": 0, "summary_emitted": False,
                         "cause": "EntryPreparation.prepare has before_json but null after_state.",
                         "correction": "Use the actual matching _apply.state for phase outcomes; "
                             "require equality when prepare.after_state is present.",
                     }},
        "source": {"source_sha": SOURCE, "tree": git("rev-parse", SOURCE + "^{tree}").decode().strip(),
                   "checkout_head": git("rev-parse", "HEAD").decode().strip(),
                   "checkout_branch": git("branch", "--show-current").decode().strip(),
                   "product_diff_from_source": [], "historical_source_sha": old,
                   "exact_delta_paths": delta,
                   "exact_delta_receipt": receipt(git("diff", "--binary", old, SOURCE)),
                   "pins": source_pins},
        "original_evidence": {
            "reference_commit": REFERENCE,
            "raw": {"local_path": str(args.raw.resolve()), **receipt(raw),
                    "published_path": ".reference/evidence/observation/run.json.gz",
                    "compressed_receipt": receipt(compressed), "decompressed_bytes_equal": True},
            "inputs": inputs, "original_receipts": original_receipts,
            "raw_case_count": 3, "raw_control_record_count": 479,
            "hooks": 71, "hooks_restored": True,
            "ordinary_resources": data["ordinary_resources"], "temporary": data["temporary"],
            "limits": data["limitations"],
        },
        "measured_historical": {
            "budget": data["budget"], "histories": histories,
            "whole_retained_advance_method_counts": total_methods,
            "whole_retained_advance_dbapi_counts": add_counts(turns, "dbapi_calls"),
            "all_observed_stages_dbapi": [
                {"case": case, "stage": stage, "operation": operation,
                 "calls": sum(x["calls"] for x in observed["admitted_dbapi"]
                              if (x["case"], x["stage"], x["operation"]) == (case, stage, operation))}
                for case, stage, operation in sorted({
                    (x["case"], x["stage"], x["operation"]) for x in observed["admitted_dbapi"]
                })
            ],
            "control_index_columns": ["raw_ordinal", "case", "before_parent_version",
                "before_parent_phase", "before_frame_ordinal", "returned_public_version",
                "returned_public_phase", "returned_checkpoint_sha256"],
            "control_index": [[x["ordinal"], x["case"], x["before"]["version"],
                x["before"]["phase"], x["before"]["frame_ordinal"], x["returned"]["version"],
                x["returned"]["phase"], x["returned"]["checkpoint_sha256"]] for x in turns],
            "control_index_note": "A complete ordinal index, not a replacement for the lossless raw. "
                "Parent phase frame is rendered as public progress phase source by _checkpoint_progress.",
        },
        "candidate_historical_occurrences": {
            "label": "historical measurements, not a savings measurement or authorized removal",
            "scope": "only final Finish.done application followed by parent finish-to-recording promotion",
            "pairs": pairs, "pair_count": 5,
            "pair_record_note": "Every selected turn is copied unchanged as a parsed JSON object "
                "from raw observation.advances[ordinal]; identities/hashes are not normalized.",
            "promotion_method_counts": add_counts(promotions, "method_calls"),
            "promotion_dbapi_counts": add_counts(promotions, "dbapi_calls"),
            "all_finish_prepare_calls_unchanged_by_candidate": 101,
            "predicted_seconds_saved": None, "guaranteed_commits_saved": None,
        },
        "static_and_proposed": {
            "status": "leave implementation out pending Native completion-discharge and Core observation contracts",
            "one_edge_only": True, "semantic_loops": False, "core_step_grouping": False,
            "current_single_child_contracts": [
                "riverhog-provenance.sqlite-semantic-finish-plan.growth/v1",
                "riverhog-provenance.sqlite-semantic-finish-application.growth/v1",
                "riverhog-provenance.sqlite-entry-replay-child-step.growth/v1",
                "riverhog-provenance.sqlite-entry-replay-advance.growth/v1",
                "riverhog-provenance.native-journal-session-advance.growth/v1",
            ],
            "hypothetical_admission": {
                "finish_prepare_call_cap": 1, "finish_apply_call_cap": 1,
                "parent_fence_call_cap": 1, "parent_publication_call_cap": 1,
                "parent_commit_api_call_cap": 1, "child_terminal_promotion_cap": 1,
                "next_recording_invocation_cap": 0, "frame_publication_cap": 0,
                "new_source_read_cap": 0, "new_child_admission_cap": 0,
                "row_and_octet_policy": "one unchanged admitted plan; no restart of _start or residual "
                    "budget and no additional semantic phase; any completion-proof work needs its "
                    "own actual control-byte/callback envelope before admission",
                "phase_policy": "one existing child prepare/application plus at most one terminal "
                    "parent-state promotion, not an unconstrained loop or a larger semantic quantum",
            },
            "blocking_contracts": [
                "When and how the completed child checkpoint/digest stops being authoritative, "
                "including deletion/replacement after commit, before another public return.",
                "Mapping of durable parent versions, public progress and Core per-step adoption/"
                "readiness/stamp/scheduling opportunities for the removed promotion boundary.",
                "Exact rollback, reply-loss, reopen, stale-worker and original resource receiving "
                "outcomes for the terminal child/parent publication.",
            ],
            "rank_counterexample_scope": "Defeats broader Native loops. The pure promotion then "
                "immediate return still performs the full Core readiness chain before a later "
                "recording call; it changes promotion timing, not proof of invalid archive acceptance.",
            "total_growth_limit": "Correction still seeds every active effective assertion and "
                "repeats generic/row/finish checks; tiny zero-adjacency inputs establish no "
                "whole-path growth closure, memory bound or constant-cost claim.",
        },
        "reported_new_diagnostics": {
            "source_comment": "https://github.com/nashspence/riverhog/issues/991#issuecomment-6890881026",
            "raw_available_or_verified_here": False,
            "manifest_sha256_reported": "8a1262afb85bd1fa831b644b86605ddc4032be6a61b2ce85b988bf65074bec3c",
            "complete_raw_sha256_reported": "dd39bd52cbe6d8e5b5e3b56c51a5f24e01fb7c4d79e8e8f72c50759fb74a743c",
            "partial_raw_sha256_reported": "bd3dae6a81b299627c0f2fde341639d760affbb1d69a47a92c0dce18bd777dd2",
            "accounting_sha256_reported": "0e7db5c818230641cc33c10d7629b4e03d33e91592b7ff8885f3bb7a5bab41f1",
            "complete": {"advances": 479, "helper_wall_seconds": 23.491,
                "advance_wall_seconds": 18.949, "advance_thread_cpu_seconds": 10.367,
                "commit_api_calls": 482, "commit_wall_seconds": 8.713,
                "commit_thread_cpu_seconds": 0.132, "hooks_restored": 77},
            "partial": {"advances": 440, "intended_advances": 479,
                "completed_case_advances": 232, "correction_advances": 208,
                "correction_intended_advances": 247, "cooperative_cap_seconds": 120,
                "completed_case_exclusive_profile_cpu_seconds": 2.741,
                "canonical_calls": 5562, "canonical_inclusive_cpu_seconds": 0.581,
                "sha256_compression_calls": 405, "compression_inclusive_cpu_seconds": 0.440},
            "interpretation": "Reports only; inclusive function times overlap. No same-run pair "
                "or throughput comparison, no fsync/wait attribution, no completion inference "
                "from the partial correction prefix and no causal saving estimate.",
        },
        "report": {"path": str(args.report.resolve()), **receipt(report_bytes)},
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output.resolve()), **receipt(args.output.read_bytes()),
                      "pairs": len(pairs), "advances": len(turns), "product_calls": 0}))


if __name__ == "__main__":
    main()
