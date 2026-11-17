# Source coverage, coordinated cutover, and exact-source qualification

This is a non-authoritative [#903](https://github.com/nashspence/riverhog/issues/903)
reference for [#1000](https://github.com/nashspence/riverhog/issues/1000), under
[#994](https://github.com/nashspence/riverhog/issues/994). It audits product source
`c28eceaa2dfec1b3712135469ba12230e74f5ffa`; it does not assert that the proposed
#995–#999 implementation is integrated. Product paths were unchanged during the
observations below. This packet belongs on the external reference branch, not as
a second product manual or work registry on `main`.

The governing choices are fixed: discover from the executable owners; resolve
every affected persistence/effect/resource delegate; retain evidence for actual
implementation or an actual-owner no-change decision; coordinate the pre-v1
cutover across all consumers; and qualify the final implementation and tested
merge source. The acceptance scope remains the aggregate durability change. It
does not reopen all of [#964](https://github.com/nashspence/riverhog/issues/964),
duplicate all of [#469](https://github.com/nashspence/riverhog/issues/469), or resume
the held [#991](https://github.com/nashspence/riverhog/issues/991) campaign.

## 1. The observed source has discovery machinery; reuse it

The reference [discovery probe](implementation/discovery_probe.py) invokes the
existing owners in the following table. It neither classifies a detected method
as safe nor runs the enumerated workloads. Generated IDs, source paths, exact
callables, source digests, and delegate edges remain the join keys for the
integration review. Changes to the executable owners must be rediscovered, not
patched into a copied census.

| Existing owner | What the reference actually obtained | Limit of that observation |
| --- | --- | --- |
| [`release.validate_release_contract` and `publication_contract`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/release.py) | Released distributions, roles, publication forms, installation roots and images | Packaging declarations do not prove installed execution. |
| [`contract_discovery`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/contract_discovery.py) | Wheel package roots and nested packages, configuration-validator reads, environment reads and source bindings | Detection is not a disposition. An unresolved read cannot be excused by an exception overlay. |
| [`contract_freeze._python_surfaces`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/contract_freeze.py) | Actual public Python member units, including inherited members and unexported mixin implementations | Public export coverage is not a private worker or resource census. |
| [`operation_qualification.operation_matrix`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/operation_qualification.py) | Current server operation identities, response authorities, official clients and CLI projections | These are the three applications owned by that matrix; other shipped process/protocol components are not exempt. |
| [`state_contract.project_owner`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/state_contract.py) | Exact relational, model-document or component-owned structure for the state declarations in `release.toml` | Nine declared state owners are not every private database, workspace, result cache or external-effect ledger. |
| [`growth_contract.declaration_projection` and `source_analysis`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/growth_contract.py) | Public bindings, private declarations, recursively resolved implementation delegates, source identity and coverage status | The source explicitly returns `evidence_status: not-executed`. Declarations are not execution or growth evidence. |
| [`ci_qualification.validation_workloads`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/ci_qualification.py) | Maintained commands, conditions and witness bindings, plus CI platform/lane routing | Enumeration does not run Docker, images, platform tests, or any workload. |

The final product gate must also use the existing contract discovery closure,
which discovers CLI trees, HTTP OpenAPI, process protocol bundles, protocol
schemas, configuration, environment, public Python units, state, operation
matrix, release metadata, growth declarations and extents. This reference does
not claim that its narrower probe ran full contract generation. The source-owned
closure test requires zero missing, stale, duplicate, multiply represented,
multiply disposed or undecided discoveries; see
[`contract_atlas.discovery`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/contract_atlas/discovery.py)
and
[`test_discovery_covers_every_owned_source_subject_without_anomalies`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_contract_discovery.py).

### Observed counts and their meaning

The machine-readable [summary](evidence/coverage_summary.json) is generated from
the raw outputs, not a maintained list. These numbers describe the pinned source:

| Projection | Observed count |
| --- | ---: |
| Released distributions | 91 |
| Python package detections | 105 |
| Public Python surfaces | 3,881 |
| Configuration-validator detections | 21 |
| Environment-read detections | 156 |
| Declared state owners | 9 |
| Current operation-matrix operations | 179: Core 126, Stove0 47, FTP spool 6 |
| Growth declarations including reachable delegates | 413 |
| Declared delegate edges | 207 |
| Enumerated validation workloads | 55 |
| Runtime images | 18 |
| Installation roots | 4 |

The public growth projection reports `owns=6`, `inherits=33`,
`nonexecutable=329`, and `unestablished=3513`. An unestablished public surface is
not automatically a defective runtime operation, and clearing every such entry
is not a new #1000 program. Conversely, a private replay/effect/persistence owner
cannot be left out because it has no public growth declaration. Follow the
actual affected entry point through its implementation to the store, process,
provider or callback that performs the work. Inspect undeclared delegates reached
by that traversal; add executable declarations or source-owned witnesses where
needed. Do not convert absence from the projection into an exemption.

The complete raw outputs are losslessly compressed JSON:
[discovery](evidence/coverage_discovery.json.gz),
[growth projection](evidence/coverage_growth_projection.json.gz), and
[growth source/delegate analysis](evidence/coverage_growth_sources.json.gz).
[Artifact digests](evidence/coverage_artifacts.json) bind compressed and original
bytes. They are review evidence, not an additional release contract or registry.

## 2. Close coverage at the actual owner

Use the live #994 child issues and their implementation PRs for work disposition.
Attach source-generated identities or exact source symbols to those decisions;
do not introduce another checked-in list of all methods or packages. Each
affected owner must have the following concrete evidence before aggregate closure:

1. **Entry and delegate.** Identify the public or installed entry point, the
   actual work executor, its effect/storage/resource delegates, and the single
   child issue that owns the shared change. A caller is not complete merely
   because the neutral policy helper has tests.
2. **State classification.** Distinguish semantic evidence, mandatory correctness
   state, optional recovery state and volatile progress/private execution. For
   every retained mandatory barrier, show the specific unsafe retry, duplicate
   effect, lost intent, identity drift, mixed generation or premature release
   that it prevents. Establish the earliest safe persistence point and the
   largest safe bounded batch. Do not relabel the former checkpoint loop as
   mandatory row installation.
3. **Replay and authority.** Bind the exact source snapshot, root/generation,
   authorization, required inputs and original operation identity. Separate
   the durable frontier from live progress. Require exact final receipt or
   publication binding; equal counts and a successful status are insufficient.
4. **Resource lifetime and growth.** Identify who receives the original process,
   worker, stream, database, retained generation, constructor result and provider
   operation. Prove per-step and total work, retained bytes/objects and receiving
   cost. Timeout, cancellation, task return, a new epoch or an empty lookup is not
   positive receiving. Include capacity exhaustion and failed receiving.
5. **Consumers and execution.** Trace the change into installed constructors,
   public models, clients, CLI/JSON projections, process protocols and Compose
   configuration. Record actual execution evidence for every affected backend
   and supported form, with failures, skips and unrun cases retained.

A **no-change** decision uses the same five checks. Accept it only when an
actual-owner witness demonstrates that the existing boundary already has the
needed semantics and resource lifetime, or that the owner has no optional
durability work on that path. Cite the concrete implementation and witness;
"read-only," "already batched," "covered by the SDK," and "a pure wrapper" are
not sufficient without resolving delegates. A source-discovered package can be
outside the durability cutover, but the disposition must state the source-backed
reason and cannot hide an affected private owner.

The implementation leads remain #995 policy/accounting, #996 Native state,
#997 Core/SDK construction and publication, #998 applications/effects, and #999
storage/recovery. #1000 joins their evidence and repairs qualification gaps; it
does not create a second policy engine, checkpoint format, publication finalizer,
provider terminality rule, receiving protocol or state migration system.

## 3. Supported state cutover is explicit and preserves obligations

The repository authorizes a coordinated pre-v1 hard cut, while prohibiting
deletion of durable user data or changes to protected commitments. It separately
requires explicit state initialization, startup structure checks and preservation
of the process/tests for upgrades between supported releases. An operational
database may contain irreplaceable information even if some of its rows are
derived. Those rules are in
[`AGENTS.md`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md),
the declared commitments in
[`release.toml`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/release.toml),
and the actual
[`state-schema` owner](https://github.com/nashspence/riverhog/tree/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/state-schema).

### Fixed deployment sequence

1. Stop accepting new work in the old deployment. Let its original executable
   receive already accepted work and owned resources where the existing protocol
   can do so. Record unresolved originals as unresolved; do not substitute a
   new-version worker to replay an ambiguous external effect.
2. Retain the old state and its identity. Before opening an existing state root
   for mutation, the new owner must validate its schema, incarnation, bindings
   and expected structure. **Populated incompatible or unknown state is refused
   unchanged.** The error identifies the owner and incompatible state; it does
   not auto-initialize, auto-delete, mark work complete, or infer that the state
   was empty. Failure to drain is a concrete deployment blocker, not permission
   to discard the liability.
3. Initialize the new baseline only through the owner’s explicit initialization
   command/process and only for an established new empty state allocation. A
   new allocation has a new incarnation; it cannot reuse an old invocation,
   workspace or publication identity or interpret an old lookup miss as proof
   of nonacceptance. For an existing deployment with retained nonreconstructible
   state, preserve it and refuse the cutover until its owner has settled it under
   the existing authority. This proposal introduces no implicit export/import
   bridge or abandoned-pre-v1 compatibility reader.
4. Verified derived state may be recreated under the new owner only from the
   exact required authority and inputs. Quarantine or dispose obsolete private
   state only after original resource receiving and after proving it contains
   no remaining mandatory source, intent, outcome, custody, construction identity
   or evidence. Retain an unresolved allocation conservatively. A missing optional
   generation may trigger bounded replay; missing mandatory inputs remain fatal.
5. Start every maintained consumer on the same new contract. Remove old field
   names, enums, status transitions, constructors, storage readers/writers and
   fallback executors together. Mixed old/new writers are unsupported. If an
   external dependency cannot change, report the blocked dependency rather than
   silently accepting the old shape.

This sequence is intentionally a closed refusal path for ambiguous retained
state. It does not promise transparent migration from every abandoned pre-v1
layout. Future supported-release upgrade machinery still remains, including
preservation, rollback/reentry after interruption and unknown-revision refusal.
Protected archive identities, encrypted object readability, provenance and
operator-owned payload/evidence remain unchanged by the operational cutover.

### State owners and current evidence boundaries

The generated release declaration contains two durable-user-content owners
(catalog and FTP custody), two durable-user-evidence owners (minisign and
OpenTimestamps ledgers), and five operational-state owners (CLI local state,
event relay, Stove0 control, Gogurt listener and target job documents). The
exact structures, fixture bindings and transition rules are in the raw discovery
output, sourced from `release.toml`; this paragraph does not replace that source.

Reconcile those declared owners with the actual Native journal/corpus workspace,
retained generation and result-cache owners, SDK prepared storage/lease owners,
adapter ledgers, observer/target/sampler controls, caches and cleanup owners
reached by the implementation traversal. In particular:

| State category encountered during the cutover | Required disposition |
| --- | --- |
| Authoritative archive objects, roots, recorded provenance, custody and evidence | Preserve identity, encryption, exact recorded meaning and independent readability. No checkpoint-format rewrite changes them. |
| Accepted request, original effect/invocation identity, terminal receipt, unresolved receiving obligation | Preserve until the existing authority positively settles it; an operational-state label grants no erasure authority. |
| Optional saved generation and dependency graph | Apply #995/#996 retention/admission and #999 physical-capacity rules. Preserve a usable complete selected generation while constructing its replacement; receive pinned readers and cleanup originals before reclamation. |
| Live private workspace or derived result/index cache | Keep physically distinct from the last retained usable generation. Loss leads only to the selected bounded replay/revalidation path with exact mandatory inputs; it never manufactures an authoritative empty result. |
| Advisory progress counters and scheduling hints | Expose live progress without promising durability. Cold recovery reports its real frontier; polling cannot execute the omitted work or acquire new authority. |
| Safety-required lease renewals, heartbeat state and fences | Preserve the owning admission, liveness and resource-lifetime contract, including mandatory persistence where that contract requires it. A heartbeat is not resource receiving; it cannot be classified as optional merely because another heartbeat is advisory. |

The first two rows need more than a successful restart of a new empty database.
Existing populated fixtures, unresolved effect state and receipt-led cleanup must
be exercised. The optional/derived rows need missing/corrupt/evicted-state tests and
actual proof that cheap execution creates no optional retained store.

## 4. Configuration and public consumers change together

Project the five #995 policy fields through each real constructor and application
configuration owner that can perform optional recovery work. Defaults stay
`adaptive`, a 120-second net saved-replay target, fraction `1/8`, unlimited
optional count and two usable retained generations. Use the shared strict
validation and parent-budget ownership selected by #995–#999; application-local
configuration can impose the selected stricter limit, not mint another budget.
Unknown keys, booleans as numeric values, nonfinite values and legacy spellings
must fail at the actual public boundary. Restart mode suppresses optional work
without suppressing mandatory correctness state or original receiving.

The 21 validator detections and 156 environment reads are candidates for review,
not a requirement to add the five fields to every schema. For example, source
catalog validation and qualification-only environment reads are not themselves
runtime policy owners. Trace each affected path to its actual constructor; apply
the policy exactly where that owner executes optional work, and reject an
implicit bypass through an installed entry point. Source numeric qualification
limits remain test conditions and do not become deployment budget defaults.

Regenerate actual OpenAPI, official client models/methods, CLI parser/JSON
projections, standalone schemas, process protocols, configuration schemas and
state structures from their owners. Update examples and Compose manifests that
instantiate those owners. Preserve semantic identities and response authorities
unless the controlling issue explicitly authorizes a changed operational field.
The public progress contract must distinguish live progress from the durable
recovery frontier. Read-only status/readback must not perform checkpointing,
revalidation, publication, cleanup, remote execution or new admission.

The existing tests already enforce exact client/server operation identities,
response contracts, CLI ownership, implementation boundaries, complete direct
dependencies and installed entry-point resolution. Run the affected cases and
the final maintained contract gate; do not hand-edit a generated contract until
the check passes. Regeneration cannot bless protected-boundary drift. Source
owners are
[`test_public_interface_parity.py`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_public_interface_parity.py),
[`test_workspace_boundaries.py`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_workspace_boundaries.py),
and
[`test_contract_freeze.py`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_contract_freeze.py).

## 5. Rebaseline private traces while retaining the safety witnesses

Tests that require a commit, checkpoint, selector adoption or disk-backed progress
write after every private advance must change with their implementation. Do not
preserve those assertions by inserting an unneeded write. Replace them with
semantic output, exact mandatory boundary and actual optional-cost assertions.
The change must still exercise the unsafe window that gave the old test value.

| Old private observation | Required new assertion |
| --- | --- |
| Every bounded replay slice has a durable workspace/checkpoint | Cheap completion has zero optional transactions/store setup; interrupted execution resumes only from a complete admitted retained generation or replays from exact mandatory input. |
| Every Native yield advances Core/SDK durable status | Live progress can advance independently; mandatory source/outcome and the eventual exact publication binding remain correct. Status polling performs no work. |
| A cold missing workspace is invariably fatal | Only a derived cache uses the typed bounded replay/revalidation path selected in #996/#997. Missing source, outcome, custody or authorization is still refused. |
| A database row or task return means cleanup is complete | The original close/worker/process/provider result has actually been received; timeout, cancellation and late failure retain the liability and capacity charge. |
| A count or final enum means complete selection | Exact identity, generation, revision, root/digest, current authorization and original receipt agree at the actual final owner. Mutation races and stale candidates are rejected. |
| A fresh empty startup proves upgrade compatibility | Populated owner fixtures retain irreplaceable data, unknown structures are unchanged on refusal, and supported-release interrupted upgrades reenter safely. |

Keep fault injection around durable intent, external effect entry, terminal
receipt, dependency registration, generation selection, final publication and
resource release. Move the hook to the corresponding new boundary when the old
microstep disappears. Ordinary raised exceptions, process death, OS/native
teardown and backend crash/power-loss guarantees are distinct evidence classes;
passing one does not satisfy the others. Detailed new owner-specific witnesses
remain with the #995–#999 leads and the safety/growth section of this packet.

## 6. Full qualification is the existing package/platform plan

The following is a reading of the maintained source plan, not a new parallel
matrix. Its raw current tuples and workloads are in
`coverage_discovery.json.gz`. Rediscover them on the final implementation rather
than freezing these counts as a contract. The platform declaration applies to
the exact published installation/image form, not every component on every OS.

| Gate and consumer | Maintained execution | Required scope and receipt |
| --- | --- | --- |
| Repository contracts, language/build checks | `make lint`, `make compile`, maintained contract/operation checks through their owning tests and `make contract-check` / `make operation-qualification` | Exact final source, declared toolchain and dependencies. All source discovery anomalies resolved; protected-boundary review remains explicit. |
| All released package tests | CI unit shards generated by `ci_unit`: `release`, `contract`, `unit-1` through `unit-4`; reproduce with `make unit-shard UNIT_SHARD=<generated shard>` | Covers released projects and actual owner delegates. No ad hoc handful of passing helpers substitutes for the plan. |
| PostgreSQL and SQLite owner behavior | Maintained SQLite owner tests; `make postgres-concurrency`; `make database-qualification` for its actual database lifecycle proof | Preserve exact state structures, transaction/CAS races, required final publication and original receiving. A SQLite-only TestClient result does not prove PostgreSQL. |
| Local archive and independent recovery | `make filesystem-recovery-qualification` | Actual encrypted filesystem archive and built recovery/client images, complete bytes/history/mapping and independent recovery after server/database unavailability. Editable in-process readers are narrower evidence. |
| Wheels and complete runtime images | `make dist-smoke`; generated image groups `core`, `observers`, `targets`, `companions` via `make image-qualification IMAGE_GROUP=<group>` | All 18 currently declared runtime images and their actual dependency closure; image IDs and installed artifact identities belong to the recorded execution. |
| Installed native consumers | `make client-platform-qualification` on `ubuntu-24.04`, `macos-15`, `windows-2025` | Source-owned native/resource/SQLite/POSIX/Windows tests, then installed-tool qualification including the maintained 12-cycle listener lifecycle. Platform-specific process/handle/close proof cannot be borrowed from Linux Python tests. |
| End-user installation roots | Existing installation qualification for `gogurt`, `a-riverhog-cli`, `a-riverhog-recovery-tool`, `a-stove0-cli` | Declared wheel-only `uv-tool` managed-Python install on `linux-x64`, `macos-arm64`, `windows-x64`; real entry points, selected provider identity and dependencies. No source/editable imports masquerading as installed consumers. |
| Whole applications and cross-service lifetimes | `make compose-shard COMPOSE_LANE=<generated lane>` for storage, ingress-custody, processing-admission, processing-e2e, processing-overlap, review-delivery and witnesses | Actual lifecycle completion, original processes/effects received, workspace/output retention, public contracts and callback/readback behavior. The plan, not polling success, owns completion. |
| First and aggregate durability performance/correctness | `make profile` scenarios selected by the measurement plan | Same-condition comparison arms, real zero-optional cheap path, long default-policy checkpoint/restart, complete output verification and terminal/cleanup tail. Runtime accounting, qualification timing and archived provenance remain separate. |
| Final integration source | Full required `.github/workflows/ci.yml` gate for exact final head and tested merge checkout | Current attempt reconciles original executions through the existing proven-origin rules, including legitimately inherited completed work; every mandatory job succeeds. Missing, skipped, cancelled, stale-source or unbound-attempt evidence is not pass. |

Source routing is
[`ci_qualification.py`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/ci_qualification.py),
[`ci.yml`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/.github/workflows/ci.yml),
[`Makefile`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/Makefile),
[`test_database_qualification.sh`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/test_database_qualification.sh),
and
[`test_filesystem_recovery_qualification.sh`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/test_filesystem_recovery_qualification.sh).
`make linux-qualification` is the maintained complete Linux route. It does not
replace native macOS/Windows evidence. Live provider tests and large held #991
campaigns still require their existing authority; enumerating a workload grants
none.

## 7. Reference observations and reproduction

The coverage probe ran with Python 3.12.3 and SQLite 3.45.1 in the dedicated
frozen `qualification-1000-env`; loaded Core, SDK, provenance, recovery,
state-schema, Stove0 and Gogurt modules all point at this checkout. Its metadata
records the source tree, lockfile digest, helper digest, numeric qualification
policy, loaded paths, stage elapsed times and `.reference` work-in-progress
status. It imports executable owners but does not run the listed workload plan.
See [execution metadata](evidence/coverage_execution.json).

The complete discovery observation took 94.672446707 seconds. Its exact helper
bytes are retained as
[the observed helper](evidence/coverage_probe_observed.py.gz). After that run,
review strengthened the current helper with final HEAD/product checks, refusal
of untracked product paths, and before/after hashing of tracked product input
bytes, then qualified its imports through the existing `scripts` owners and
the actual `scripts.ci_unit.CI_SHARDS` definition. The full discovery scan was
not repeated for these guard/import-only changes.
Eleven focused guard cases and five subtests passed in 0.05 seconds of pytest
time and 1.126472738 seconds of whole-process time. Their real before/after
product snapshots match across 2,564 tracked files. See
[guard invocation and source hashes](evidence/coverage_guards_command.json),
[guard JUnit](evidence/coverage_guards.xml),
[guard stdout](evidence/coverage_guards_stdout.txt), and
[guard timing](evidence/coverage_guards_timing).
After the import correction, the same eleven guard cases and five subtests
passed again in 0.05 seconds of pytest time and 1.189116372 seconds of whole-process
time. The final run also loaded the qualified discovery modules from this exact
checkout without running another scan. Its
[invocation and input hashes](evidence/coverage_guards_final_command.json),
[JUnit](evidence/coverage_guards_final.xml),
[stdout](evidence/coverage_guards_final_stdout.txt), and
[timing](evidence/coverage_guards_final_timing) remain distinct from the first
guard run. Both final owned Python files passed Ruff, formatting and strict
mypy with `--follow-imports=silent`; the
[final static record](evidence/coverage_static_final.json) is not a `make lint`
result. The manifest binds their final hashes separately from the preserved
observed-scan helper.

Five unchanged owner cases passed in **2.26 seconds of pytest time** and
**3.947408696 seconds of whole-process time**, with zero failures/errors/skips:

- explicit initialization produces the expected verified state;
- forward upgrade preserves state and reaches its sole head;
- interrupted upgrade rolls back and can be reentered;
- unknown revision refuses without destroying operational state;
- every declared v1 state owner projects its exact component-owned structure.

The first four execute local SQLite state-schema behavior; the fifth projects
declared structures, including PostgreSQL DDL, but does **not** execute PostgreSQL.
These are baseline preservation witnesses, not a completed new-format cutover
or installed/platform qualification. Exact command, numeric conditions, source
status and original receipts are in
[command metadata](evidence/coverage_state_command.json),
[JUnit](evidence/coverage_state.xml),
[stdout](evidence/coverage_state_stdout.txt), and
[timing output](evidence/coverage_state_timing).

Reproduce the source snapshot with the exact product checkout and a locked
environment whose editables point to it. The final published reference commit
contains additional `.reference` files; it is not a clean c28 product checkout.
Use a separate checkout at c28 and the helper from this packet, or use the exact
new implementation SHA when generating integration evidence. Write output
outside the checked-out product tree:

```sh
/workspace/scratch/939b84c043b0/qualification-1000-env/bin/python \
  .reference/implementation/discovery_probe.py \
  --repo /workspace/scratch/939b84c043b0/riverhog-1000 \
  --output /workspace/scratch/939b84c043b0/coverage-reproduction \
  --source-sha c28eceaa2dfec1b3712135469ba12230e74f5ffa
```

The current helper rejects a different HEAD, changed tracked product paths,
untracked nonignored product paths, evidence inside the product tree or imports
from another worktree. It also marks the observation failed if the final source
checks or tracked input hashes differ. Its caller must exclusively own product
paths throughout; boundary hashes cannot detect transient edits restored between
observations. Concurrent reference writers and Git-ignored caches are outside
that product-source ownership claim. It records reference WIP, rather than
presenting such a run as the clean maintained profile. The
maintained source gate additionally requires exclusive ownership and a clean
checkout; do not relax
[`qualification_source.py`](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/qualification_source.py)
to make a qualification command accept this packet.

The focused state command is recorded verbatim in the metadata. For reruns,
derive numeric environment values with
`scripts.validation_numeric.bind_numeric_environment` and
`qualification_numeric_policy`, set external `TMPDIR`, `--basetemp` and
`RIVERHOG_CI_TIMING_DIR`, and run the two recorded test inputs with
`-p scripts.ci_timing -o junit_family=xunit1` and a JUnit path. Do not copy the
numeric qualification bounds into product configuration. The reference helper
has no parallel inventory algorithm. Its focused guard tests cover refusal for
wrong source, product edits, untracked product code, changed input bytes, deleted
inputs, an invalid repository and an unsafe or already occupied output path.
These tests use isolated files and mocked Git command boundaries rather than
repeating the discovery scan.

## 8. Finite closure and explicit stopping point

Start with the first integrated Native/default-policy path and the Core/SDK F1
owner immediately. On that slice establish exact source, zero optional work for
cheap completion, a real useful long checkpoint/restart, correct fresh/warm/
evicted readers, receiving and terminal tail. Continue through the affected
application and storage owners as each implementation lands; do not wait for a
perfect census before measuring the first path, or defer safety qualification
until all code is written.

Aggregate closure requires all affected owners to have implemented or evidenced
no-change decisions; the mandatory-barrier review to be complete; the relevant
fault windows and full lifecycle growth witnesses to pass; same-condition
comparison evidence to explain results and any corrected over-admission; every
changed public/config/state/installed consumer to be coordinated; and the full
required final-source and tested-merge qualification to be green. Record
unexecuted platform/provider/backend claims as unestablished. Do not turn blocked
infrastructure, a wrapper timer, a count delta, a pure model or this reference
branch into passing implementation evidence.

The owning #1000 closure comment should link those original receipts and exact
sources, identify any unrelated work deliberately left outside #994, state the
parent #994 acceptance disposition and record the resulting #991 precedence
decision. A new failure in a required owner remains an acceptance blocker; an
unrelated future improvement remains with its existing owner. Once those finite
conditions are met, stop. No generic scheduler, persistent registry, portable
storage rewrite, new release authority or additional benchmark campaign is
needed to close this qualification issue.
