# Performance accounting — noncontractual

Generated from [performance_objectives.py](../../scripts/performance_objectives.py); edit that source, not this page.

These engineering objectives and observations do not create, extend, interpret, override, or relax an external contract. A recorded target does not create a release gate. No result or passing run is claimed here.

A goodput comparison uses newly completed useful bytes divided by its complete operation interval. A reference is another measured sample on the same declared path and workload; nominal link capacity and raw transport rate are not references. The checker can compare declarations, but cannot independently attest that an operator's context fingerprints describe physical conditions truthfully. Missing or incompatible evidence is not evaluated, never passed. Absolute rates accompany every ratio. The 0.90 fraction remains a report-only candidate pending workload, baseline, and release interpretation.

Comparison context JSON has `format = riverhog-performance-comparison-context/v1`, a shared UUID `comparison_id`, SHA-256 fingerprints for `workload_sha256`, `environment_sha256`, and `path_sha256`, plus `byte_domain`, `completion_boundary`, `cache_state`, and positive `concurrency`. These are declarations to review, not independently verified physical facts. The reference is a prior v2 result from the same producer and comparison session.

The transfer profiler passes `RIVERHOG_PERFORMANCE_RUN_ID` and `RIVERHOG_PERFORMANCE_RECEIPT` to its command. A command that verifies actual completion may write a JSON receipt with `format = riverhog-performance-completion/v1`, that exact `run_id`, exact `completed_bytes` and `completed_items`, and `verified = true`. Exit success alone leaves completion unverified and cannot meet an objective. The storage probe verifies the exact readback itself.

## Report-only candidate objectives

### transfer-goodput

Metric: newly completed useful payload per end-to-end wall time (`bytes/second`).

Workload: Named network scenario and exact declared workload fingerprint.

Useful-work numerator: Verified newly completed logical payload bytes; exclude resume work already complete and retransmitted bytes.

Timed interval: Command start through verified completion, including setup and teardown.

Reference: Another measured, completed run of the same operation path.

Comparable when: Same scenario, workload, byte count/domain, completion boundary, path, environment, cache state, concurrency, and comparison session.

Candidate ratio: `candidate/reference >= 0.90`. Enforcement: report-only candidate, owned by `scripts/transfer_profile.py::main`.

Executable source: [`scripts/transfer_profile.py::main`](../../scripts/transfer_profile.py).

### storage-upload-goodput

Metric: completed stored-object payload per upload wall time (`bytes/second`).

Workload: One synthetic object through a sequential public storage-adapter path.

Useful-work numerator: Exact completed stored-object payload bytes.

Timed interval: Begin-write request through validated complete-write response.

Reference: Another measured upload with matching declared context.

Comparable when: Same payload size, segment maximum, path, environment, cache state, concurrency, and comparison session; compare upload with upload only.

Candidate ratio: `candidate/reference >= 0.90`. Enforcement: report-only candidate, owned by `tests/harness/storage_adapter_goodput_probe.py::run`.

Executable source: [`tests/harness/storage_adapter_goodput_probe.py::run`](../../tests/harness/storage_adapter_goodput_probe.py).

### storage-read-goodput

Metric: verified stored-object payload per read wall time (`bytes/second`).

Workload: Read after writing one synthetic object through the public storage adapter.

Useful-work numerator: Exact read and hash-verified stored-object payload bytes.

Timed interval: Open exact revision through complete read, hash, and stream close.

Reference: Another measured read with matching declared context.

Comparable when: Same payload size, segment maximum, path, environment, cache state, concurrency, and comparison session; compare read with read only.

Candidate ratio: `candidate/reference >= 0.90`. Enforcement: report-only candidate, owned by `tests/harness/storage_adapter_goodput_probe.py::run`.

Executable source: [`tests/harness/storage_adapter_goodput_probe.py::run`](../../tests/harness/storage_adapter_goodput_probe.py).

## Observations without objectives

- **recovery-transfer-rate**: Recovery command bytes, items, and elapsed time have no accepted numerical target. [Source: `scripts/transfer_profile.py::main`](../../scripts/transfer_profile.py).
- **operation-cold-cli-startup**: Cold CLI startup samples have no accepted latency target. [Source: `scripts/operation_qualification.py::_cold_cli_timings`](../../scripts/operation_qualification.py).
- **operation-local-api-client-wall**: Local API and client timing samples have no accepted per-operation speed target. [Source: `scripts/operation_qualification.py::_load_operation_timings`](../../scripts/operation_qualification.py).
- **provider-qualification-elapsed**: Provider qualification start, phase, and completion timestamps are observations. [Source: `scripts/provider_qualification.py::evidence_from_checkpoint`](../../scripts/provider_qualification.py).
- **stove0-scale**: Scale-smoke elapsed time, CPU, memory, and size observations have no rate target. [Source: `scripts/test_compose_smoke.sh`](../../scripts/test_compose_smoke.sh).
- **transfer-phase-telemetry**: Segment and phase totals are diagnostics, not unique end-to-end goodput. [Source: `riverhog/src/riverhog_core/throughput.py`](../../riverhog/src/riverhog_core/throughput.py).

## Independently owned bounds and guards

- **database-regression-guards**: Fixture-local memory, plan-work, and latency envelopes remain enforced by database qualification. [Source: `scripts/database_qualification.py`](../../scripts/database_qualification.py).
- **gogurt-listener-regression-guards**: Fixture-local traced-allocation ceilings remain with the listener tests. [Source: `some-implementations/gogurt/application/tests/test_listener.py`](../../some-implementations/gogurt/application/tests/test_listener.py).
- **extent-and-correctness-bounds**: Contract extents and correctness/resource checks retain their independent authorities. [Source: `scripts/extent_contract.py`](../../scripts/extent_contract.py).
