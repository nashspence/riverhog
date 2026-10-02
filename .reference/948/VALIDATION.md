# Reference validation

This report applies to the reference material only. It is not Riverhog release,
protocol-conformance, GPU, or scheduler-integration qualification.

## Executed locally

Environment: CPython 3.13.5, Linux; Python standard library only.

From the repository root (or the supplied reference tree):

```sh
python -m unittest discover -s .reference/948 -p 'test_*.py' -v
python -m py_compile .reference/948/lifecycle.py .reference/948/test_lifecycle.py
```

Result: **28 tests passed**. Repeated the same suite ten additional times: all ten
runs passed, 280 additional test executions. Compilation and reference-file newline/
trailing-whitespace checks passed. No dependency install or network service was
needed. Tests use temporary SQLite databases and synthetic permits; no real data,
archive mutation, NVIDIA device, or external lease service was used.

The tests exercise explicit state interleavings, not real scheduling latency or
thread/process concurrency. A model input asserting stopped workers is not evidence
that any real GPU consumer was stopped. Reopening SQLite is not power-loss testing.

## Repository inspection

Read current #903 and #948 through the GitHub connector; audited source is pinned to
`239985271c9ec24942073f4005dd18da621d0251`. The bounded source map in INTEGRATION.md
records inspected ranges and exact blobs. No unverified earlier API sketch was
adopted as an existing repository contract.

A direct public git clone was attempted and failed on DNS resolution in the
execution container. Source inspection proceeded through the GitHub connector.
Consequently no full production checkout/dependencies or repository-wide tests were
available locally.

## Not performed

`make lint`, `make unit`, `make dist-smoke`, `make build`; full generated-contract or
state-baseline validation; Python 3.12 execution; PostgreSQL/multi-process CAS tests;
real HTTP clients/servers; active capability renewal and callback refresh; actual
preview/nested planning changes; remote broker fault injection; NVENC encoding;
process-tree crash/fencing; power-loss durability; fleet fairness and load testing.

Those checks remain integration work, even if this reference commit has no failing
GitHub checks. No production or frozen contract files are modified by this branch.

## Publication verification

Local patch generation/application was checked in a disposable Git repository;
all six resulting file blob hashes matched the tested reference files. This checks
the patch round trip, not production compatibility or an authoritative base checkout.

The producer previously encountered a blocked create-blob publication attempt.
Publication is now performed through the GitHub Connector from the exact audited
parent. Remote tree verification, the resulting reference SHA, and any check state
are recorded in #948 after publication rather than asserted here. Native Development
linkage is established separately when the publication interface supports it, or its
absence is recorded in #948 per #903.
