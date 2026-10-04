# CI gate reshaping research

Owning issue: #954  
Convention: #903  
Audited source: `main@29d16e99d52bda0f308495fd7be519c1e321b735`

This note captures the evidence and reasoning behind the handoff. It is not an
accepted repository design. Issue #954 and subsequent maintainer/integration
decisions control.

## Objective function

The repository should optimize for preserving implementation context, not for
minimizing runner minutes at the expense of delayed failures.

A two-hour gate that reports against the exact pushed SHA while the development
episode is still active can be cheaper than a ten-minute gate plus a later
qualification failure that requires reconstructing intent, architecture, and
test semantics. Standard GitHub-hosted runner time is intentionally generous
for public repositories; the design should use that capacity responsibly to
parallelize meaningful work rather than omit it.

The desired transformation is therefore:

> same meaningful proofs, same exact source SHA, same development episode,
> less accidental serialization and less duplicate setup.

Fast early feedback remains useful, but it is not a substitute for the complete
gate.

## Authoritative repository posture at the audited base

`AGENTS.md` requires focused tests while iterating followed by:

```text
make lint
make unit
make dist-smoke
make build
```

After push, the exact pushed SHA must complete its GitHub Actions checks before
work is handed back. Direct commits to `main` are the current pre-v1
convergence rail. Any redesign should strengthen this local-first / remote-
independent relationship, not create two unrelated definitions of "validated."

The CI workflow currently runs three broad groups:

- repository targets on Linux;
- native client/platform qualification on Linux, macOS, and Windows;
- one image-build job per declared image target.

`release/v1` currently requires the individual contexts named in
`release.toml`, including CodeQL, all image jobs, platform jobs, and each
repository target.

## Measured CI evidence

### Successful main run #903

Workflow run:
https://github.com/nashspence/riverhog/actions/runs/37197047070

Source:
`29d16e99d52bda0f308495fd7be519c1e321b735`

Approximate job wall times derived from the Actions API:

| Job | Wall time |
| --- | ---: |
| make compose-smoke | 148.0 min |
| make unit | 53.3 min |
| make postgres-concurrency | 10.3 min |
| client platform (windows-2025) | 9.5 min |
| make filesystem-recovery-qualification | 8.1 min |
| client platform (macos-15) | 5.4 min |
| client platform (ubuntu-24.04) | 4.2 min |
| most image jobs | about 1-3 min |
| make dist-smoke | 1.7 min |
| make lint | 0.6 min |
| make compile | 0.4 min |
| make c2sp-vectors | 0.4 min |

The workflow ran from approximately 10:57Z to 13:25Z, so the compose job was
effectively the workflow critical path.

### Recent variance

The two preceding successful main runs show that the problem is not a single
outlier:

| Run | Source | unit | compose |
| --- | --- | ---: | ---: |
| #899 | cc2a5f8... | 33.3m | 102.1m |
| #901 | 802ed382... | 32.2m | 75.9m |
| #903 | 29d16e99... | 53.3m | 148.0m |

The redesign should preserve timing evidence after the refactor so later growth
is visible.

## Unit lane: the long tail is concentrated

The #903 unit job reported:

```text
3376 passed, 1 skipped in 3167.61s (0:52:47)
```

The slowest observations were:

| Duration | Phase | Test |
| ---: | --- | --- |
| 1414.98s | call | `test_release_pages_path.py::test_trusted_preparation_offline_signature_immutable_history_and_main_pages` |
| 1130.85s | call | `test_operation_qualification.py::test_release_disposable_selection_satisfies_current_observation_requirements` |
| 851.04s | call | `test_contract_pages.py::test_pages_assembles_versions_semantically_and_preserves_published_bytes` |
| 781.37s | call | `test_documentation_workflows.py::test_author_can_plan_write_exact_markdown_and_review_an_incomplete_candidate` |
| 428.49s | setup | `test_extent_contract.py::test_transform_output_policy_does_not_import_upload_batch_or_store_count_limits` |
| 427.55s | setup | `test_contract_freeze.py::test_decorated_client_source_resolves_to_its_repository_definition` |
| 427.36s | setup | `test_console_entrypoints.py::test_published_console_entrypoint_help_is_side_effect_free` |
| 419.39s | setup | `test_release.py::test_release_evidence_is_complete_and_minisign_verified` |

The important inference is not merely that these tests are expensive. The
session-scoped fixture in `tests/conftest.py` calls
`contract_atlas.generation.build_candidate()`, and other release fixtures
build on that generated closure. Under pytest-xdist, session fixtures are
created independently inside workers. More workers can therefore increase
total generated-contract work.

The existing unit default:

```text
-n 4 --dist=loadscope
```

is reasonable for ordinary unit tests but is not automatically optimal for a
suite where a handful of large release/contract tests dominate.

### Recommended unit experiment

Do not begin with `-n auto`.

Instead:

1. Define a canonical test inventory from the existing `TESTS` roots.
2. Define 3-4 coarse semantic shards.
3. Add an executable completeness check proving every canonical collected
   node/file belongs to exactly one shard.
4. Measure each shard with worker counts 1, 2, and 4 where fixture cost is
   material.
5. Compare `loadscope`, `load`, and `worksteal` only after the shard
   boundary exists.
6. Keep the local `make unit` full-suite behavior intact unless an exact
   equivalent aggregate replaces it.

A useful first partition to benchmark is:

- **release/publication**: release, release-pages, publication/evidence;
- **contract/documentation**: contract atlas, contract pages/freeze/generation,
  documentation preparation/workflows;
- **operation/extent/policy**: operation qualification, extent/workspace policy,
  other high-setup modules;
- **broad unit remainder**: everything else.

Those names are intentionally conceptual. The integration agent should derive
the exact module inventory from collection and timing data rather than copy a
fragile hand-maintained list from this note.

### Why not hash node IDs into N shards?

A stable hash gives exact coverage but can distribute tests that share an
expensive generated fixture across every shard, multiplying setup. It also
obscures semantic debugging. A duration-aware semantic partition plus an
executable coverage check better matches this repository.

## Compose lane: it is a qualification suite

`scripts/test_compose_smoke.sh` is 1,359 lines at the audited base. It
contains roughly 85 direct Compose command references and orchestrates several
distinct lifecycle proofs.

The sequence includes, among other things:

- Garage/bootstrap and storage-adapter conformance;
- filesystem/cache adapter goodput and restart continuation;
- encrypted archive integration tests;
- deterministic concurrency/interleaving tests;
- Riverhog application startup and restart;
- a Stove0 database and substantial multi-image Stove0 build;
- native observer/target probing;
- FTP spool/listener ingestion and restart;
- explicit durable admission-boundary restart proofs;
- end-to-end processing with multiple outputs;
- Review0 and rclone delivery;
- restart/replay of processing and delivery;
- Minisign and OpenTimestamps witness lifecycle.

The script itself documents an intentional serialized-work budget:

```text
smoke_completion_timeout=$((600 + 480 * smoke_file_count))
```

with a default `smoke_file_count=16`. The comment explains that overlapping
routes produce four target outputs per fixture input and that target jobs are
serialized.

This is not merely slow shell overhead. Some of the long path is meaningful
application work. The optimization therefore needs to distinguish:

- meaningful sequential behavior inside one proof;
- unrelated proofs serialized only because they share one shell script;
- repeated image/build/setup work that can be cached;
- intentionally serialized application behavior that should remain tested.

### Candidate compose proof boundaries

These are starting hypotheses for extraction, not accepted cuts:

#### A. Storage boundary

Keep together:

- Garage/bootstrap where required;
- storage adapter conformance;
- goodput;
- archive/filesystem continuation across restart;
- Riverhog app startup/restart and persistent application state.

This is a coherent Riverhog/storage lifecycle.

#### B. Ingress custody

Keep together:

- FTP listener/spool setup;
- exact event acquisition;
- pathname reuse/custody assertions;
- listener/adapter restart and replay cleanup;
- handoff into Riverhog.

Do not split before/after a restart whose semantics depend on the same durable
state.

#### C. Stove0 processing

Keep together:

- selected observer/target stack;
- classification/admission;
- explicit durable-boundary restart checks;
- controller/worker processing;
- overlapping-route behavior;
- completion/history publication.

This will likely remain the heaviest scenario. It should be measured before
further subdivision.

#### D. Review/delivery

Make the minimum self-contained fixture necessary to prove:

- ordinary finalized review collection;
- delivery admission;
- rclone materialization;
- manifest/content verification;
- restart/replay.

If it needs a prepared upstream collection, create that deterministically
inside the scenario rather than relying on mutable state from another job.

#### E. Independent witnesses

Create one bounded finalized collection, then prove Minisign and
OpenTimestamps witness state/restart/retention. These can likely be independent
from the large Stove0 processing scenario once the input fixture is made
explicit.

### Thin end-to-end spine

If extraction causes every scenario to replace real upstream behavior with
fixtures, retain one modest but semantically complete cross-component spine
that proves Riverhog ingestion -> Stove0 -> produced collection/delivery.

The purpose is cross-component wiring coverage, not scale. Do not lower
cardinality in an existing proof unless its semantics are independently
accounted for.

## Build behavior and cache strategy

The separate image matrix already uses:

```text
cache-from=type=gha,scope=<target>
cache-to=type=gha,scope=<target>,mode=max
```

and most warm #903 image jobs completed in roughly 1-3 minutes.

By contrast, compose qualification invokes ordinary Compose builds inside its
own runner. That is a clear area for experimentation.

Because each GitHub-hosted job gets a fresh VM, loaded local Docker images
cannot be shared across jobs without an explicit transfer. Avoid treating
image tar artifacts as the first solution:

- GitHub Free artifact storage is 500 MB;
- Docker image archives can be large;
- upload/download/retention adds another failure and lifecycle surface.

Prefer each compose scenario building/loading only its required images while
using the same target-scoped BuildKit GHA caches. Cache hits accelerate the
build but do not become evidence: the resulting image still has to be loaded
and exercised by that exact job.

Do not introduce a mutable registry tag as an implicit cross-job authority.

## Recommended remote topology

A plausible steady-state topology is:

```text
CI
├─ static/repository
│  ├─ lint
│  ├─ compile
│  ├─ c2sp-vectors
│  ├─ postgres-concurrency
│  ├─ filesystem-recovery
│  └─ dist-smoke
├─ unit (3-4 coarse shards)
├─ compose qualification (4-5 scenario shards)
├─ client platforms
│  ├─ linux
│  ├─ macos
│  └─ windows
├─ image inventory (existing target matrix, bounded max-parallel)
└─ CI gate
   └─ requires every mandatory group above
```

This is not a recommendation to run every possible leaf at once. Use explicit
caps. One defensible first budget is:

- unit matrix: max-parallel 3 or 4;
- compose matrix: max-parallel 4 or 5;
- image matrix: max-parallel 4 to 6;
- client platform matrix: existing 3.

GitHub's account-level concurrency limit will queue excess work, but the
workflow should express restraint itself rather than relying on the platform
to throttle it.

The goal is roughly 6-10 substantial Linux jobs in active qualification, not
20 tiny jobs.

## Stable aggregate gate

A final job should have a stable protected-context name independent of shard
names. Example concept:

```yaml
ci-gate:
  name: CI gate
  if: always()
  needs:
    - repository
    - unit
    - compose
    - client-platforms
    - images
  steps:
    - run: verify every required needs.*.result == success
```

The concrete implementation should be tested in
`tests/unit/test_github_actions.py`.

Why this helps:

- leaf names can change as timing is rebalanced;
- protected-branch configuration remains stable;
- failures retain detailed leaf checks;
- one green status means the full exact-SHA CI graph succeeded.

Do not use an aggregate to hide a skipped mandatory leaf. If a required group
is conditionally skipped, the aggregate should reject that result unless the
condition is itself an accepted repository contract.

## Local topology

The local path and remote path should share targets, not YAML logic.

A useful shape is:

```text
make gate
  -> maintained Make targets / scenario targets
  -> complete portable semantic gate

GitHub matrices
  -> invoke the same leaf targets independently
  -> aggregate exact-SHA result
```

The integration agent should decide whether `make gate` includes all
Linux-portable CI qualification or preserves the existing four-command
portable baseline plus separately named deep local qualifications. The
important rule from issue #954 is that the repository must still have an
unambiguous local pre-main validation rail and CI sharding must not become the
only definition of coverage.

macOS and Windows qualification are naturally remote supplements when the
developer host cannot execute them.

## Governance migration

At the audited base, the `release/v1` ruleset requires many individual
GitHub Actions contexts and `release.toml` records that exact list.

A safe migration is additive first:

1. introduce the new aggregate while keeping existing leaf context names;
2. observe a complete successful exact-SHA run;
3. add/validate governance tests for the aggregate;
4. update `release.toml` and live required contexts coherently;
5. only then rename/remove obsolete leaf contexts.

This avoids a protected branch waiting on a context that no longer exists.

CodeQL should remain independently required unless the repository explicitly
decides otherwise; the CI aggregate should not claim ownership of another
workflow's authority by accident.

## GitHub resource facts checked for this handoff

Current GitHub documentation checked on 2026-10-04 states:

- standard GitHub-hosted runners are free and unlimited for public repositories;
- public `ubuntu-24.04` is a 4 CPU / 16 GB RAM standard runner;
- GitHub Free has a 20 concurrent standard-runner limit and a 5 macOS-job cap;
- default cache storage is 10 GB per repository;
- GitHub Free artifact storage is 500 MB;
- larger runners are billed even for public repositories.

References:

- https://docs.github.com/en/actions/reference/runners/github-hosted-runners
- https://docs.github.com/en/enterprise-cloud@latest/actions/reference/limits
- https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching
- https://docs.github.com/en/billing/concepts/product-billing/github-actions

The design should stay on standard runners.

## pytest-xdist facts checked for this handoff

The current pytest-xdist documentation was reviewed for distribution modes and
fixture behavior:

- `loadscope` groups tests by module/class scope;
- `load` distributes tests as workers become available;
- `worksteal` is intended to improve balancing where runtimes differ;
- each xdist worker is a separate pytest process, so session-scoped fixtures
  are not globally singleton across all workers.

References:

- https://pytest-xdist.readthedocs.io/en/stable/distribution.html
- https://pytest-xdist.readthedocs.io/en/stable/how-to.html

This is why fixture-generation cost must be part of the shard benchmark.

## Things deliberately not recommended

### Do not move compose to nightly-only

That saves gate time by shifting failure discovery out of the development
episode, which is opposite the project's cost model.

### Do not use changed-file path filtering for semantic correctness

Riverhog's contracts span packages, applications, generated release evidence,
and workflow/governance tests. A changed-path heuristic can create exactly the
kind of delayed context reconstruction this redesign is meant to avoid.

### Do not reduce the 16-file default merely because it is slow

First establish what invariant the multiplicity proves. If fewer files prove
the same functional invariant and a separate scale lane already proves the
cardinality requirement, reduction is reasonable. Runtime alone is not the
justification.

### Do not create 20 one-test runners

Public runner capacity makes coarse parallelism affordable. It does not make
VM provisioning, cache churn, log volume, and service fairness irrelevant.

### Do not make caches evidence

A cache can accelerate reconstruction of an exact artifact. The job must still
validate the artifact and behavior for the current exact source.

## Definition of a successful redesign

The successful redesign is not the topology drawn in this document. It is a
measured system where:

- the same meaningful coverage remains attached to every relevant pushed SHA;
- the maintainer still runs the required local validation before main;
- failures are reported in the same development episode;
- unit and compose long tails no longer unnecessarily serialize unrelated
  proofs;
- session/build setup duplication is bounded by measurement;
- standard public Actions capacity is used strongly but intentionally;
- governance has one stable aggregate CI meaning;
- the final exact-SHA integration run is fully green.

A sub-hour remote critical path is a useful engineering target. It is not an
acceptance criterion that overrides correctness.
