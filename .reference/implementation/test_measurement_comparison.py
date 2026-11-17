from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from measurement_comparison import (
    ARMS,
    DEFAULT_POLICY,
    Counters,
    Observation,
    compare_block,
    summarize_blocks,
)

from scripts import performance_measurement as existing


def block(
    *,
    restart_ns: int = 70,
    adaptive_ns: int = 80,
    block_number: int = 10,
) -> list[Observation]:
    context = {
        "format": existing.CONTEXT_FORMAT,
        "comparison_id": str(UUID(int=1)),
        "workload_sha256": "1" * 64,
        "environment_sha256": "2" * 64,
        "path_sha256": "3" * 64,
        "byte_domain": "logical-payload",
        "completion_boundary": "durable-user-result",
        "cache_state": "mixed-declared",
        "concurrency": 2,
    }
    result = []
    for order, (arm, elapsed) in enumerate(zip(ARMS, (100, restart_ns, adaptive_ns), strict=True)):
        run_id = str(uuid4())
        result.append(
            Observation(
                arm=arm,
                block_id=str(UUID(int=block_number)),
                order=order,
                sample=existing.sample(
                    scenario="collection-publication",
                    workload="F1-C1-K2-Q1",
                    context=context,
                    completed_bytes=65_536,
                    elapsed_seconds=elapsed / 1_000_000_000,
                    completion_verified=True,
                    run_id=run_id,
                ),
                source_sha=("a" if arm == "legacy" else "b") * 40,
                source_clean=True,
                artifact_sha256=("a" if arm == "legacy" else "b") * 64,
                executor_sha256=("c" if arm == "legacy" else "d") * 64,
                instrumentation_sha256="e" * 64,
                conditions_sha256="f" * 64,
                clock_origin_id=run_id,
                input_kind="fresh-publication",
                result_identity_sha256=str(order) * 64,
                receipt_sha256=uuid4().hex + uuid4().hex,
                policy=None if arm == "legacy" else {**DEFAULT_POLICY, "recovery_mode": arm},
                result_ns=elapsed,
                received_ns=elapsed + 30,
                counters=Counters(
                    worker_cpu_ns=200,
                    optional_store_initializations=0,
                    optional_attempts=0,
                    optional_successes=0,
                ),
            )
        )
    return result


def test_exact_threshold_and_tail_are_separate_observations() -> None:
    rows = block()
    rows[2] = replace(rows[2], received_ns=160)
    report = compare_block(rows)
    assert report["status"] == "compared" and report["report_only"] is True
    assert report["durable_result"] == {
        "signed_delta_ns": 10,
        "clipped_d_ns": 10,
        "d_over_adaptive": {"numerator": 1, "denominator": 8},
        "target_at_most_one_eighth": True,
        "equivalent_restart_over_seven": True,
    }
    assert report["received"]["signed_delta_ns"] == 60
    assert report["received"]["target_at_most_one_eighth"] is False
    assert report["tail_ns"] == {"legacy": 30, "restart": 30, "adaptive": 80}
    assert report["counter_differences"]["worker_cpu_ns"] == 0
    assert report["counter_differences"]["sync_attempts"] is None


def test_old_slow_executor_cannot_supply_the_optional_tax_denominator() -> None:
    report = compare_block(block(restart_ns=7, adaptive_ns=10))
    assert report["legacy_to_restart_signed_ns"] == -93
    assert report["durable_result"]["d_over_adaptive"] == {"numerator": 3, "denominator": 10}
    assert report["durable_result"]["target_at_most_one_eighth"] is False


@pytest.mark.parametrize(("adaptive_ns", "within"), [(799, True), (800, True), (801, False)])
def test_one_eighth_and_restart_over_seven_are_exact(adaptive_ns: int, within: bool) -> None:
    report = compare_block(block(restart_ns=700, adaptive_ns=adaptive_ns))
    assert report["durable_result"]["target_at_most_one_eighth"] is within
    assert report["durable_result"]["equivalent_restart_over_seven"] is within


def test_faster_adaptive_retains_signed_difference_and_zero_clipped_cost() -> None:
    report = compare_block(block(restart_ns=80, adaptive_ns=70))
    assert report["durable_result"]["signed_delta_ns"] == -10
    assert report["durable_result"]["clipped_d_ns"] == 0
    assert report["durable_result"]["d_over_adaptive"] == {"numerator": 0, "denominator": 1}


def test_paired_spread_does_not_hide_clipping_bias_or_invent_confidence() -> None:
    result = summarize_blocks([block(restart_ns=80, adaptive_ns=70), block(block_number=11)])
    observed = result["durable_result"]
    assert observed["signed_delta_range_ns"] == [-10, 10]
    assert observed["signed_delta_mean_ns"] == {"numerator": 0, "denominator": 1}
    assert observed["mean_of_pairwise_clipped_d_ns"] == {"numerator": 5, "denominator": 1}
    assert observed["clipped_mean_signed_delta_ns"] == {"numerator": 0, "denominator": 1}
    assert result["uncertainty"] == "descriptive-paired-spread-no-confidence-interval"


def test_repeated_adverse_observations_need_disposition_but_are_not_test_failures() -> None:
    result = summarize_blocks(
        [block(restart_ns=7, adaptive_ns=10, block_number=i) for i in range(3)]
    )
    assert result["report_only"] is True and result["status"] == "compared"
    assert result["durable_result"]["observed_target_misses"] == 3
    assert result["durable_result"]["repeated_miss_needs_owner_disposition"] is True


@pytest.mark.parametrize("missing", ARMS)
def test_missing_arm_is_not_an_inferred_success(missing: str) -> None:
    rows = [row for row in block() if row.arm != missing]
    report = compare_block(rows)
    assert report["status"] == "not-compared"
    assert report["missing_arms"] == [missing]
    assert report["run_ids"] == [row.sample["run_id"] for row in rows]


def test_missing_tail_does_not_turn_into_zero_cost_or_full_receiving() -> None:
    rows = block()
    rows[2] = replace(rows[2], received_ns=None)
    report = compare_block(rows)
    assert report["durable_result"]["clipped_d_ns"] == 10
    assert report["received"] is None
    assert report["tail_ns"]["adaptive"] is None
    assert report["receiving_comparison_reason"] == "original-receiving-incomplete"


@pytest.mark.parametrize("field", ["source_sha", "artifact_sha256", "executor_sha256"])
def test_restart_and_adaptive_must_use_the_same_redesigned_code(field: str) -> None:
    rows = block()
    rows[2] = replace(rows[2], **{field: "9" * (40 if field == "source_sha" else 64)})
    assert compare_block(rows)["reason"] == "redesigned-executors-differ"


def test_mode_labels_cannot_relabel_one_executor_as_all_three_arms() -> None:
    rows = block()
    rows[0] = replace(
        rows[0], source_sha=rows[1].source_sha, executor_sha256=rows[1].executor_sha256
    )
    assert compare_block(rows)["status"] == "not-compared"


@pytest.mark.parametrize(
    "field", ["instrumentation_sha256", "conditions_sha256", "interruption_sha256"]
)
def test_different_conditions_or_instrumentation_do_not_compare(field: str) -> None:
    rows = block()
    rows[2] = replace(rows[2], **{field: "8" * 64})
    assert compare_block(rows)["reason"] == f"incomparable-{field}"


def test_existing_context_validator_and_matching_rules_are_reused() -> None:
    rows = block()
    changed = {**rows[2].sample, "context": {**rows[2].sample["context"], "cache_state": "warm"}}
    rows[2] = replace(rows[2], sample=changed)
    assert compare_block(rows)["reason"] == "incomparable-context"
    changed["context"]["extra"] = "not admitted"
    with pytest.raises(existing.PerformanceError, match="unexpected fields"):
        compare_block(rows)


@pytest.mark.parametrize("field", DEFAULT_POLICY)
def test_all_five_effective_default_settings_are_required(field: str) -> None:
    rows = block()
    selected = dict(rows[2].policy)
    selected.pop(field)
    rows[2] = replace(rows[2], policy=selected)
    with pytest.raises(existing.PerformanceError, match="five effective policy settings"):
        compare_block(rows)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("recovery_mode", "restart"),
        ("replay_target_seconds", 121),
        ("max_optional_overhead_fraction", 0.25),
        ("max_optional_checkpoints", 2),
        ("retained_checkpoint_generations", 3),
    ],
)
def test_default_adaptive_arm_cannot_use_a_custom_policy(field: str, value: object) -> None:
    rows = block()
    rows[2] = replace(rows[2], policy={**rows[2].policy, field: value})
    with pytest.raises(existing.PerformanceError, match="selected defaults"):
        compare_block(rows)


def test_restart_optional_work_is_invalid_but_unknown_is_not_measured_zero() -> None:
    rows = block()
    rows[1] = replace(rows[1], counters=Counters())
    assert compare_block(rows)["restart_optional_zero_observed"] is False
    rows[1] = replace(rows[1], counters=Counters(optional_store_initializations=1))
    with pytest.raises(existing.PerformanceError, match="performed optional recovery"):
        compare_block(rows)


def test_fresh_publications_may_have_new_identities_but_captured_results_cannot_conflict() -> None:
    rows = block()
    assert len({row.result_identity_sha256 for row in rows}) == 3
    assert compare_block(rows)["status"] == "compared"
    rows = [replace(row, input_kind="captured-immutable") for row in rows]
    with pytest.raises(existing.PerformanceError, match="conflicting results"):
        compare_block(rows)
    rows = [replace(row, result_identity_sha256="7" * 64) for row in rows]
    assert compare_block(rows)["status"] == "compared"


@pytest.mark.parametrize(
    "change",
    [
        {"result_ns": True},
        {"result_ns": 81},
        {"received_ns": 79},
        {"source_clean": False},
        {"clock_origin_id": str(UUID(int=700))},
        {"counters": Counters(optional_attempts=True)},
        {"counters": Counters(commit_attempts=2, commit_returns=3)},
        {"counters": Counters(sync_attempts=2, sync_returns=3)},
    ],
)
def test_invalid_supplied_evidence_remains_an_error(change: dict[str, object]) -> None:
    rows = block()
    rows[2] = replace(rows[2], **change)
    with pytest.raises(existing.PerformanceError):
        compare_block(rows)


@pytest.mark.parametrize("change", [{"completion_verified": False}, {"bytes_per_second": 0.5}])
def test_unverified_or_forged_samples_fail_the_existing_receipt_contract(
    change: dict[str, object],
) -> None:
    rows = block()
    rows[2] = replace(rows[2], sample={**rows[2].sample, **change})
    with pytest.raises(existing.PerformanceError):
        compare_block(rows)


def test_duplicate_original_receipt_cannot_count_as_independent_work() -> None:
    rows = block()
    rows[2] = replace(rows[2], receipt_sha256=rows[1].receipt_sha256)
    with pytest.raises(existing.PerformanceError, match="receipt reused"):
        compare_block(rows)


def test_repeated_execution_cannot_count_as_another_paired_observation() -> None:
    rows = block()
    with pytest.raises(existing.PerformanceError, match="same execution repeated"):
        summarize_blocks([rows, rows])


def test_different_artifacts_across_repetitions_are_not_pooled() -> None:
    first, second = block(), block(block_number=11)
    second = [replace(row, artifact_sha256="8" * 64) for row in second]
    report = summarize_blocks([first, second])
    assert report["status"] == "not-compared"
    assert report["reason"] == "mixed-conditions-or-artifacts"
    assert len(report["blocks"]) == 2


def test_unmatched_block_stays_visible_in_the_summary() -> None:
    report = summarize_blocks([block(), block(block_number=11)[:2]])
    assert report["paired_observations"] == 1
    assert report["omitted_or_unmatched_blocks"] == 1
    assert report["all_declared_blocks_compared"] is False


def test_interruption_costs_are_reported_separately_from_the_normal_tax_target() -> None:
    rows = [replace(row, interruption_sha256="6" * 64) for row in block()]
    rows[1] = replace(
        rows[1], counters=Counters(repeated_network_bytes=4096, paid_effect_attempts=1)
    )
    rows[2] = replace(rows[2], counters=Counters(repeated_network_bytes=0, paid_effect_attempts=1))
    report = compare_block(rows)
    assert report["kind"] == "interrupted"
    assert report["durable_result"]["target_at_most_one_eighth"] is None
    assert report["counter_differences"]["repeated_network_bytes"] == -4096
    assert report["counter_differences"]["paid_effect_attempts"] == 0
    assert summarize_blocks([rows])["durable_result"]["observed_target_misses"] == 0


def test_optional_failure_and_cleanup_costs_are_retained_as_distinct_attribution() -> None:
    rows = block()
    rows[1] = replace(rows[1], counters=Counters(optional_lifecycle_ns=0, optional_failed_ns=0))
    rows[2] = replace(
        rows[2],
        counters=Counters(
            optional_attempts=2,
            optional_successes=1,
            optional_lifecycle_ns=30,
            optional_failed_ns=12,
            optional_cleanup_ns=8,
        ),
    )
    report = compare_block(rows)
    assert report["counter_differences"]["optional_lifecycle_ns"] == 30
    assert report["counter_differences"]["optional_failed_ns"] == 12
    assert report["counter_differences"]["optional_cleanup_ns"] is None
    assert report["durable_result"]["clipped_d_ns"] == 10
