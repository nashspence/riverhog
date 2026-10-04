# CI gate integration map

Owning issue: #954  
Convention: #903  
Audited source: `main@29d16e99d52bda0f308495fd7be519c1e321b735`

This is a concrete implementation map for the integration agent. It is not an
instruction to cherry-pick this reference branch: the branch intentionally
does not modify production/workflow files.

## Recommended integration order

The safest order is:

1. add observability without changing coverage;
2. establish unit partition completeness machinery;
3. introduce unit shards and benchmark;
4. extract compose scenarios one at a time, preserving the monolith as a
   comparison oracle until parity is demonstrated;
5. move compose builds onto target-scoped GHA BuildKit cache;
6. add the stable aggregate CI gate;
7. run the old and new shapes together on at least one exact SHA where
   practical;
8. migrate `release.toml` / live `release/v1` required contexts;
9. remove superseded monolithic workflow invocations only after parity;
10. update `AGENTS.md` only as needed to name the final local gate.

This order favors proof of equivalence over an all-at-once workflow rewrite.

## File-level map

### `.github/workflows/ci.yml`

Expected changes:

- preserve exact checkout with `inputs.ref || github.sha`;
- preserve top-level `cancel-in-progress: true`;
- split the current `unit` target out of the generic repository matrix if a
  dedicated unit matrix is introduced;
- split `compose-smoke` out of the generic repository matrix into a
  dedicated scenario matrix;
- add explicit `max-parallel` to unit/compose/image matrices;
- keep client-platform matrix behavior intact unless measurements justify a
  separate change;
- retain the current target-scoped image cache behavior;
- add a final stable `CI gate` job with `if: always()`;
- do not use workflow `paths` filters to decide semantic coverage;
- retain bounded failure artifacts only where diagnostically valuable.

Candidate shape, subject to integration measurements:

```text
repository             max-parallel 4
unit                   max-parallel 3
compose-qualification  max-parallel 4
client-platforms       existing 3
images                 max-parallel 4-6
ci-gate                1
```

The account may still have more than ten active jobs because groups overlap.
The point is that each matrix advertises a reasonable bound and no matrix is
deliberately expanded to the Free-plan ceiling.

### `Makefile`

Keep target behavior useful outside Actions.

Likely additions:

- one target per compose proof scenario;
- an aggregate `compose-smoke` that executes every scenario locally in a
  deterministic order, preserving the existing command name if feasible;
- optional unit-shard targets for reproducing one CI shard locally;
- a unit-shard completeness target;
- a canonical local aggregate (`gate`, `validate`, or another repository-
  appropriate name) if issue #954 resolves to one command.

Do not implement the local gate as "invoke GitHub Actions locally." The Make
targets are the source-level interface; Actions should call them.

A possible compatibility-preserving shape is:

```make unit
make unit-shard SHARD=release
make unit-shard SHARD=contract
make unit-shard SHARD=rest
make unit-shards-check

make compose-storage
make compose-ingress
make compose-stove0
make compose-review
make compose-witnesses
make compose-smoke   # aggregate all of the above
```

Names are suggestions, not contracts.

### Unit partition helper

Suggested new file:
`scripts/ci_unit_partition.py` (name flexible).

Responsibilities:

- own the CI unit shard definitions;
- expose a command that prints test roots/node files for one shard;
- expose a verification command that compares shard ownership with the
  canonical unit collection/inventory;
- reject an unowned test module;
- reject duplicate ownership;
- produce deterministic output for tests/workflow consumption.

Avoid tying ownership to changed files or a timing database that silently
changes behavior. Duration data may inform checked shard definitions, but
coverage ownership must remain inspectable.

A simple robust first implementation can operate at test-file granularity.
There is no need to partition individual parametrized node IDs unless module
boundaries prove too coarse.

Recommended policy test:
`tests/unit/test_ci_unit_partition.py`.

It should validate:

- every test file under the canonical `TESTS` roots is assigned exactly once,
  subject to explicit non-unit exclusions such as the C2SP environment-gated
  vector file if the existing canonical unit behavior already handles it;
- shard names used in `ci.yml` match the helper;
- no shard is empty;
- the broad local `make unit` roots remain the union authority.

Do not make the test shell out to a full 50-minute pytest run. Validate the
declared file inventory structurally; the actual shard jobs prove execution.

### `tests/conftest.py`

Do not casually rewrite the generated-contract fixture merely for timing.

First measure which shards actually require:

- `generated_contract_closure`;
- `documented_source_plan`;
- `release_contract_factory`.

The current session-scoped fixture is expensive and xdist-local. A good shard
layout may be enough to limit duplicate generation without introducing
cross-process fixture cache complexity.

If a shared on-disk generated candidate is later introduced, require:

- exact source/input identity in its directory/key;
- atomic construction;
- no worker accepting incomplete output;
- deterministic verification on read;
- cache behavior that cannot turn stale generated output into test authority.

That optimization should be reviewed as test-fixture correctness, not just
performance.

### `scripts/test_compose_smoke.sh`

Treat this file as the behavioral oracle during extraction.

Do not begin by cutting the shell file into five line ranges. First identify
the durable state each assertion depends on.

A low-risk extraction pattern is:

1. create reusable shell helpers for setup/cleanup/environment construction;
2. move one semantically independent tail scenario (witnesses are a likely
   first candidate) into its own executable script;
3. make the old `compose-smoke` call that scenario so local aggregate
   coverage is unchanged;
4. add a CI job invoking the scenario independently;
5. once parity is proved, avoid double execution in CI while keeping the local
   aggregate;
6. repeat for the next scenario.

Potential directory:
`scripts/compose_smoke/` with a small common library plus executable scenario
scripts. Keep names descriptive rather than numbered.

Do not share mutable scenario state between Actions jobs.

### `scripts/_compose_env.sh`

This is a natural home only for generic Compose environment helpers.

Potential extensions:

- timing/phase helper used by scenario scripts;
- image-build helper capable of consuming an Actions-provided cache-aware build
  path without changing local semantics.

Do not turn this file into scenario policy.

### `docker-bake.hcl`

Possible changes:

- add groups representing images needed by each compose scenario;
- reuse the existing exact image targets rather than define duplicate Docker
  build semantics;
- keep per-target cache scopes where possible.

Be careful with a multi-target bake invocation and wildcard cache settings:
one broad shared scope can make cache behavior worse and less intelligible.
The current per-target scopes are a useful property to preserve.

The integration agent should compare:

- independent target cache scopes;
- scenario-group builds that still assign target-specific scopes;
- plain Compose builds with BuildKit cache configuration.

Choose the least complex option that materially improves wall time.

### `tests/unit/test_github_actions.py`

This is an enforceable policy owner and should be updated with the workflow.

Add assertions for:

- explicit unit shard set;
- explicit compose scenario set;
- configured `max-parallel` values;
- every shard invoking the maintained Make/script target;
- final `CI gate` job using `if: always()`;
- final gate depending on every mandatory CI group;
- aggregate verification rejecting failed/cancelled mandatory dependencies;
- pinned action SHAs remain pinned;
- existing least-privilege permissions remain intact;
- no path-based selective gate is introduced.

If the workflow uses generated JSON matrices, test the generator instead of
copying a second inventory into this policy file.

### `release.toml`

Do not edit this first.

Once the aggregate check is proven, the intended steady state is likely a much
smaller `required_checks` list, for example:

```text
Analyze (actions)
Analyze (python)
CI gate
```

That is a design direction, not an accepted final list. CodeQL currently has a
separate workflow/authority and should remain explicit unless issue #954
records a different decision.

The advantage is that unit/compose/image leaf names can evolve without
rewriting branch protection each time.

### `scripts/github_governance.py`

The current exact comparison between the live release/v1 ruleset and
`release.toml` remains useful. Do not weaken it merely because the list gets
shorter.

Migration work should preserve:

- integration ID validation;
- strict/current-head required status behavior;
- exact required-context agreement;
- release/v1 pull-request policy.

### `tests/unit/test_github_governance.py`

Update fixtures/expected required contexts only when `release.toml` changes.
Add a regression for the new aggregate context if it is special in any way.

### `AGENTS.md`

Only update after the local gate naming has settled.

The durable policy should remain simple:

- focused tests while iterating;
- complete portable local validation before committing to main;
- exact pushed SHA must pass required GitHub checks;
- CI failure is fixed before handoff.

If one new command replaces the current four-command list, its Make definition
must make the underlying coverage inspectable.

## Unit experiment plan

Use the current main suite as the control.

### Phase U0: baseline

Capture:

- wall time;
- total collected/passed/skipped;
- top durations;
- worker count/distribution mode;
- number and elapsed time of generated-contract fixture constructions if easy
  to instrument.

### Phase U1: semantic three-way split

Candidate:

- `release`;
- `contract-doc`;
- `remainder`.

Run with conservative worker counts. A first trial of 2 workers for the heavy
shards and 4 for remainder is reasonable.

Compare total compute and critical path. If a 20-minute test remains alone on
the critical path, a fourth shard may be justified.

### Phase U2: distribution tuning

Within each shard, compare only modes relevant to that shard:

- `loadscope` where module locality matters;
- `worksteal` where durations are highly uneven;
- `load` as a simple baseline.

Reject a configuration that saves a few wall minutes by exploding repeated
fixture generation or flakiness.

### Phase U3: stability

Run the candidate several times. The current 32 -> 53 minute variance is large
enough that one warm run is not evidence.

## Compose experiment plan

### Phase C0: add markers

Record elapsed times for:

- base/test image construction;
- storage bootstrap/conformance;
- Riverhog app startup/restart;
- Stove0 image construction;
- ingress lifecycle;
- processing completion;
- restart/replay;
- review/delivery;
- witnesses;
- teardown.

No behavior change.

### Phase C1: extract witnesses

Witnesses are at the current tail and already use independent Compose files.
Make them self-contained first if their dependency on the prior finalized
collection can be replaced with a small real finalized Riverhog input.

Compare the independent witness scenario against the old monolith assertions.

### Phase C2: extract review/delivery

Build the smallest real upstream state needed for Review0/rclone semantics.
Keep restart/replay within the same scenario.

### Phase C3: separate storage/ingress from Stove0 processing

This is the point where the largest gain may appear, but it also has the
highest semantic risk. Preserve one end-to-end spine.

### Phase C4: cache-aware builds

Once scenario target sets are known, adopt GHA BuildKit caching per exact image
target. Measure cold and warm behavior.

## Aggregate gate implementation detail

The aggregate should consume job-level results, not re-run tests.

One implementation pattern is to serialize `needs` to JSON and reject every
mandatory dependency whose result is not `success`.

Do not rely on shell globbing over environment variables whose absence could
look like success. Make the expected dependency set explicit in the workflow
policy test.

Matrix jobs are represented to downstream `needs` by their job ID, so a
failed matrix child should make the matrix job result fail. Verify this with a
temporary branch/run before changing branch protection.

## Resource budget

The workflow already has many image jobs. New unit/compose shards should not
simply be added with unconstrained matrices.

Suggested starting caps:

| Group | max-parallel |
| --- | ---: |
| repository quick lanes | 4 |
| unit | 3 |
| compose | 4 |
| images | 5 |
| client platforms | 3 existing entries |

These caps are intentionally below the 20 concurrent Free-plan ceiling while
still taking strong advantage of standard public runner capacity.

If queue observations show that a different balance gives a materially shorter
critical path without a large compute multiplier, tune it.

## Caching and artifacts

Use caches for reconstructible acceleration only.

Good candidates:

- mise/uv dependency caches already managed by the pinned action;
- BuildKit target-scoped GHA caches.

Avoid by default:

- Docker image tar artifacts;
- giant generated-contract artifacts passed between unit shards;
- mutable registry tags used as same-run synchronization.

Bounded failure evidence remains appropriate, as the client-platform Gogurt
job already demonstrates.

## Rollout / branch protection

Because `release/v1` is protected by exact required context names:

1. land new jobs without removing old contexts;
2. get a green run;
3. verify `CI gate` is emitted on push/PR/workflow_call cases used by release
   qualification;
4. update release governance declaration/tests;
5. update live ruleset through the normal authorized governance path;
6. verify another green run;
7. remove obsolete workflow leaf names only after they are no longer required.

The integration agent should not use a force/bypass path to get around a
missing status during this migration.

## Handoff completion criteria for the integration agent

The reference has done its job when the integration agent can answer, with
evidence:

- What exact tests/scenarios make up the local semantic gate?
- How is complete unit shard ownership mechanically verified?
- Which compose proofs are independent, and which restart/replay sequences
  remain deliberately serial?
- How many generated-contract builds occur per unit run before and after?
- Which images does each compose scenario build, and which cache scopes serve
  them?
- What is the maximum intended Actions fan-out?
- What single stable status proves complete CI?
- How is release/v1 governance migrated without a missing-context deadlock?
- What are the median/range critical-path timings over several representative
  green runs?
- What meaningful coverage, if any, changed? The preferred answer is none
  unless issue #954 records an explicit semantic decision.
