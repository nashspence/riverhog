# Disposable one-member cost observer

This probe is research material, not a second maintained profiling command or a
product optimization. The delivered run is already complete: read
[RESULTS.md](../../RESULTS.md) and [NEXT_SLICE.md](../../NEXT_SLICE.md) first.
Reproduce only to resolve a concrete attribution question on a reconciled source.

The bootstrap activates only when both `RIVERHOG_991_DIAGNOSTIC_DIR` is set and
`sys.orig_argv` identifies exactly the existing
`python -m tests.support.qualification.publication_profile` helper. It leaves
the outer profile command and original-process receiver unchanged. Product bytes
stay unchanged, but imports, wrappers and counters change diagnostic conditions.

From a clean reference checkout with repository-pinned tools available:

```bash
evidence_root="$(mktemp -d /tmp/riverhog-991.XXXXXX)"
mkdir -p "$evidence_root/tmp" "$evidence_root/observer"
export TMPDIR="$evidence_root/tmp"
mise x python uv -- uv run --locked --all-packages --group dev python \
  .reference/experiments/probe/selfcheck.py "$evidence_root/selfcheck"

PYTHONPATH="$PWD/.reference/experiments/probe:$PWD${PYTHONPATH:+:$PYTHONPATH}" \
RIVERHOG_991_DIAGNOSTIC_DIR="$evidence_root/observer" \
make profile \
  'UV_RUN=mise x python uv -- uv run --locked --all-packages --group dev' \
  args="--scenario collection-publication \
  --publication-workload .reference/experiments/one-member.json \
  --output $evidence_root/profile --finalization-timeout-seconds 300"
```

The repository root in `PYTHONPATH` is intentional: startup must resolve source
owners before `runpy` executes the helper. The hook patches its imported owners,
not an unused second copy of the helper module. The UV override selects pinned
Python/uv without changing Make recipes, the workload or acceptance checks.
Use the existing resource rail and keep output outside the checkout. The
300-second condition bounds finalization, not arbitrary startup/cleanup time.
Cancellation must still receive the actual original helper and its resources.

The observer writes one `diagnostic-<pid>.json` at interpreter exit. Accept
attribution only after matching its source/observer identities, three pass
ordinals, valid/overflow/unfinished-span fields and expected hooks against the
maintained profile's actual success, exact output and original-helper/workspace
receipts. Source cleanliness includes `.reference/`; commit any local observer
revision before producing a clean-source result. An observer cap invalidates
attribution; it never limits accepted archive size or converts partial work to
success. Missing or invalid diagnostic output is not a successful measurement.

## Scope and overhead

Coarse publication, append, finish, TestClient request and verification scopes
coexist with journal advance/ledger, catalog execute and admitted Native cursor
counters. Same-thread direct child intervals are subtracted for exclusive
accounting. Different threads retain separate observed lifetime IDs. Cross-phase
spans are charged to their start phase; phase CPU is process-wide. Verification
ordinals must be reconciled with the profile's fresh/warm/evicted ordering.

Catalog cursor events omit fetch, commit and pool/connection work. Admitted
Native SQL is only a subset: raw disposable JournalStore, closure and worklist
SQLite is outside those counters, though broader owner spans can include it.
Returned-row counts are not engine row visits. `executemany` counts calls without
pre-consuming its generator. SQL text, parameters, payloads and identity values
are not retained. Raised calls retain a count, not an error-family attribution.

The clock window starts after dependency imports; earlier startup is outside it.
Bootstrap time overlaps recorder startup and cannot be added blindly. Per-thread
exclusive CPU covers selected scopes, not complete thread lifetimes. The helper's
unassigned CPU remains explicit. Same-thread exclusive wall is not a concurrent
critical-path trace, and wall minus CPU does not identify a particular lock or
IO wait. Queue/ready/active/retained-worker states are not collected here.

Empty-call calibration and recorded bookkeeping CPU are partial overhead
observations, not a correction to subtract from production time. The probe adds
no thread-entry/profile hook, process or sampler, and no per-call file writes.
It has finite thread/bucket/depth/phase budgets; raw output records the limits
and any failure. The selected workload is one member; this is not a large-run
instrumentation prescription.

## Semantic checks

`selfcheck.py` exercises actual admitted SQLite grants/cursors, one-shot generator
consumption, SQL and SQLAlchemy errors, return and BaseException identity,
iterator exhaustion, nested clocks and original thread joining. It restores its
patched methods/events. The delivered no-workload activation check intentionally
returned status 2 with an incomplete-verification diagnostic; that expected
negative check proves activation without pretending a publication ran.
