# Supplied Stove0 implementation conventions

This is researched design input for #965, not an independent source of repository authority. The current issue and subsequent maintainer decisions control. The rule IDs below organize this finite handoff; do not introduce them as a public protocol taxonomy or a second normative registry on main.

## 1. The ontology: roles first, execution traits second

A distribution, executable, image, HTTP interface, semantic contract, operation, job, and artifact are different things. Sharing one does not equate the others. The current source contains 38 Stove0 distributions, 20 console executables, and 10 declared runtime images. The supplied Compose file has 15 services, including controller, worker, and state-management processes that are not independent extension implementations.

Use the actual descriptor/protocol, result kind, public entrypoint, and code-owned authority to determine a role. Use execution traits only to apply machinery such as native supervision or a workspace. Do not infer semantic roles from directory names, image names, `a-` prefixes, imports alone, or whether a test uses mocks.

| Source-defined role | Current supplied subjects | Owns | Must not acquire |
| --- | --- | --- | --- |
| Stove0 coordination and operator interface | `stove0-server` serve/controller/worker/state; `stove0-api-client`; `stove0` CLI | Workflow, claims, scheduling contacts, accepted evidence, operator projections | Extension payload execution, GPU scheduling, archive custody |
| Content observer | ExifTool, FFprobe, magic, Riverhog provenance, filename-prefix sidecars | Exact observation under its contract and supplied read authority | Transformation/publication, implicit classification or retirement authority |
| Direct collection-transform target | Opus, NVENC AV1+Opus | Selected transformation and complete authorized collection production | Core scheduler ownership or authority over unrelated inputs |
| Delegating collection-transform target/application | Review0 | Sampling coordination, validated sample reuse, final collection publication | Sampler's GPU policy or authority merely from sharing an image/workspace |
| Workflow external-effect target | Rclone | A selected, verified external delivery and its effect receipt | A fabricated output collection or automatic replay of an uncertain effect |
| Artifact-free departure effect | Existing protocol, client, persistent support and tests; no deployed provider found | Exact departure delivery and receipt under its own protocol | Workflow artifact inputs or an invented new deployed provider |
| Review sampler | Opus sampler, NVENC sampler | Exact assigned-input/window sampling and sample result | Riverhog claims, publication, source retirement, catalog, archive credentials |
| Pure semantic/provider and authoring code | Seven observer contract/validator providers; media-archive contract; Review contracts/planner; recipe compiler | Schemas, semantic validators, vectors, deterministic authoring/compilation | Runtime resource selection, network execution, service state |
| Transport/support/tooling | Four client libraries; protocol/support libraries; schema and conformance executables | Their declared transport/runtime/tool contracts | Another component's semantics or a stronger proof claim than actually exercised |

Observer subclasses are meaningful but share one observation lifecycle: ExifTool/FFprobe/magic consume payload bytes through approved readers and native tools; the provenance observer interprets exact canonical documentary evidence; filename-prefix sidecars derives relations from accepted locator evidence. The latter two are not exempt from bounded execution or optional local admission, but must not start reading artifact payload merely to resemble the native observers.

Targets also have distinct result kinds. Review0 is a collection-producing target even though it delegates encoding. Rclone is a workflow external-effect target, not an artifact-free departure target. Samplers are neither observers nor Riverhog-producing targets. No common superclass may blur those distinctions.

## 2. Common conventions

### C01 — Complete, source-backed coverage

Reconcile the maintained workspace, release-exposed distributions, console scripts, registered providers, actual applications/bindings, and shipped compositions. Every applicable subject must have an exercised fixture or a specifically tested non-applicability reason. Compare discovered membership with exercised membership in both directions; missing, stale, duplicate, renamed, and newly added subjects must fail the coverage check. Unknown classification is unfinished work, not an exemption. Use existing discovery and pytest collection rather than another allowlist or policy manifest. The reference inventory reader captures this snapshot only; it is not the production discovery solution.

### C02 — Separate identities and authority

Keep independently selected components independently described and authenticated. An operation ID is not an implementation ID, image ID, local registration name, job ID, or resource ID. Pairing an encoder and sampler in an image does not merge their descriptors, endpoints, credentials, state ownership, or acceptance authority. A rule requiring an implementation convention is repository policy; an exposed setting/endpoint still has its own real public contract.

### C03 — Exact values and correctly scoped commitments

Use the owning canonical JSON, digest, timestamp, path, and exact-scalar definitions. Unbounded logical byte extents must use the existing exact scalar representation; bounded counts and durations retain their declared domains. Test booleans, nonfinite values, negative values, large values and canonical encodings at their owner. Do not normalize unrelated fields solely because they have similar names.

Exclude execution lock paths, lock names, credentials, queue timing, transport locations and deployment topology from semantic operation/work commitments. Do not demand that all configuration changes leave all digests unchanged: descriptor/image changes, explicit portable intent, accepted operational limits already committed by a contract, and Rclone's selected destination identity/naming rules can legitimately qualify requests/results. Verify invariance only while fixing all semantic inputs and implementation identities and varying explicitly operational configuration. Never replace an effect's destination commitment with a private locator or remove it as supposedly incidental topology.

### C04 — Reuse mechanics without borrowing implementation authority

Protocol/contract packages stay dependency-light. Clients consume the public role contract. Runtime support owns generic admission, metadata transport, state safety and process containment. Native command construction and domain semantics remain with their owning components. Pure validators and planners do not pull execution runtimes. Do not import a target's private FFmpeg helper into its sampler. Put genuinely identical mechanics in an appropriately scoped shared package or retain tiny owner-local adaptations; a universal plugin/service framework is not required.

### C05 — One configuration model appropriate to its shape

Follow the accepted structured-configuration convention: substantial operator policy/registration graphs use one schema-validated YAML document with semantic checks; simple process wiring remains scalar CLI/environment input. Stove0, Review0 and Rclone retain their structured documents. Do not force trivial leaf services or pure libraries to acquire YAML. Do not add an unrestricted environment override layer over structured policy. Parser, published schema, defaults, runtime and deployment wiring must agree.

### C06 — Conventional process bindings and operational controls

Use the bootstrap table in section 3. One reusable definition owns each common concept/domain/default, while each executable owns its external prefix/binding. Expose the same applicable controls rather than constructor-only features. Preserve justified workload defaults, not accidental wiring omissions. Pure tools and clients do not acquire daemon flags. Unknown CLI/YAML input is rejected at the owning boundary; retired component-specific aliases must not remain silently active. Do not reject unrelated operating-system environment variables.

### C07 — File-referenced executable secrets

Supplied executable/deployment configuration uses mounted secret-file references: `token_file` in structured YAML or `<PREFIX>_TOKEN_FILE` for a simple leaf. Remove direct secret-value `<PREFIX>_TOKEN` input in the pre-v1 cut; do not keep precedence aliases. Programmatic constructors/clients may still receive already-resolved credentials from their owner. Read and validate secrets before allocating execution resources; never log them or copy them into descriptors, accepted documents, workspace names, exception details or reports. Runtime capability renewal is not configuration and must not be persisted as a secret-bearing accepted job. Distinguish service authentication from job/attempt authority.

### C08 — Side-effect-free introspection and exception-safe lifetime

`--help` and `--version` work without secrets, state, native binaries, network access or GPU/lease acquisition. Validate selected configuration, paths, semantic relations, credentials and identity before meaningful service initialization. Resource construction must unwind on every later startup failure. Normal shutdown and startup failure close clients, metadata workers, dispatchers, state owners and file admissions in ownership order. Closing may be idempotent; it must not unlock before containment. Do not block the ASGI event loop while joining consumers. Use a focused common lifecycle helper where appropriate; do not enforce a particular class hierarchy.

### C09 — Real HTTP binding and declared surface agree

Public models and operation declarations own paths/methods, request/response shapes, status/error pairs, authentication and metadata transport. Actual app routes, clients, generated OpenAPI and conformance tools must agree. Authenticate execution/control and metadata routes before reading/staging payload-bearing bodies. Keep explicitly public liveness/readiness/introspection limited to safe metadata. Unadvertised methods/selectors are rejected rather than silently accepted. Expected errors must be declared by the actual operation, not merely appear in a global vocabulary. Include all five observers, all maintained targets and both samplers in common role tests; test Stove0 operator routes through their own operator contract.

### C10 — Health is not a job and not an encode

`/health/live` reports process liveness with the conventional service identity and HealthOut. `/health/ready` reports the ability to accept the declared work, including correctly configured local dependencies, within a finite control budget. A contended optional lease means work can queue, not that the service is unhealthy. Readiness cannot encode, consume a scarce execution permit, create a workspace, restore archive content, mutate an external destination or wait indefinitely. Safe bounded version/encoder-list metadata probes are not payload encoding; test that distinction rather than imposing a blanket ban on the executable name. Every emitted readiness failure, including 503, is present in OpenAPI with the standard error shape.

### C11 — Bounded control, complete logical inputs

Accept/refresh, descriptor/preflight, status and cancel stay bounded. Enclosing scheduler/coordination budgets are not reset for each nested HTTP request. Claim/authority maintenance remains independently due during backoff. Metadata-only preflight cannot read payload or acquire execution resources. Large valid canonical documents use existing resumable/staged transport; large sets use the owning bounded continuation contracts. No local response/body/queue budget may silently truncate work or become a new total logical-size cap. Preserve selected schemas' genuine limits, such as an explicit sample-output budget, rather than erasing them under the banner of parity.

### C12 — Preserve outcome distinctions

Keep malformed input, unavailable admission, content inapplicability, infrastructure failure, cancellation request, interrupted work, unknown external commit, and proved completion distinct. A timeout is not proof of nonexecution; cancel acknowledgment is not proof of consumer stop. Preserve already-proved completion when cancellation/lease loss arrives late. Clients distinguish declared remote rejection, transport failure and invalid response; observer-specific unsupported-semantics remains meaningful. Do not force one global status enum across different result protocols.

### C13 — Clients, operator CLI and schema/conformance commands

Client methods use the owning operation definitions, bounded control/metadata transport and exact result validation. They do not independently interpret recipes or manufacture remote errors. The operator CLI preserves complete JSON results and meaningful human projections, explicit paging, current enum/default values and honest exit status. Machine-only schema/conformance tools remain machine-only where declared; do not invent an unnecessary human mode. All 20 executables retain working side-effect-free help/version and installed-artifact entrypoints. A descriptor inspection is not a completed lifecycle qualification.

## 3. Exact bootstrap convention

Here `P` means the executable's existing public component prefix (`STOVE0`, `REVIEW0`, or the corresponding `A_STOVE0_*` / `A_REVIEW0_*` prefix), not a prefix guessed from image packaging. Python/YAML field names use snake_case; CLI uses kebab-case; environment uses the same concept in upper snake case. CLI wins over its simple process environment binding, then the declared default. Substantial structured policy has no parallel override tree.

| Applicable concept | CLI / environment | Selected meaning/default |
| --- | --- | --- |
| Listen host | `--host` / `P_HOST` | Loopback `127.0.0.1`; shipped container commands explicitly select `0.0.0.0` |
| Listen port | `--port` / `P_PORT` | 8080; validated integer TCP port |
| Structured document | `--config` / `P_CONFIG` | Required where the component has structured config; no artificial leaf document |
| Leaf service credential | `--token-file` / `P_TOKEN_FILE` | Required file reference; structured services use YAML `token_file` instead |
| File-owned job state | `--state-root` / `P_STATE_ROOT` | `/var/lib/<service>`; no fake file state root for the database-owned Stove0 app |
| Payload scratch/assigned workspace | `--workspace` / `P_WORKSPACE` | Existing owner-specific default; `/run/<service>` or the explicit Review0 shared-workspace composition |
| Optional local exclusive resource | `--execution-lease-file` / `P_EXECUTION_LEASE_FILE` | None; immediate admission when absent |
| Payload execution slots | `--maximum-workers` / `P_MAXIMUM_WORKERS` | Positive integer; preserve the current provenance observer's four-worker default, otherwise the current one-worker defaults |
| Pending accepted-job capacity | `--maximum-pending-jobs` / `P_MAXIMUM_PENDING_JOBS` | Positive integer, current 256-job default; explicit admission rejection, never partial acceptance |
| Admission contact allowance | `--admission-probe-seconds` / `P_ADMISSION_PROBE_SECONDS` | Finite positive seconds, current 1.0 default |
| Deferred-probe cadence | `--admission-retry-seconds` / `P_ADMISSION_RETRY_SECONDS` | Finite positive seconds, current 1.0 default; unrelated progress is mandatory, strict FIFO is not promised |
| Disposable terminal retention | `--terminal-state-retention-seconds` / `P_TERMINAL_STATE_RETENTION_SECONDS` | Positive integer seconds, current 2,592,000 default; never authorizes deleting unresolved effects or protected evidence |
| Build identity | `P_SOURCE_REVISION`, `P_IMAGE_ID` | Existing exact role descriptors; unknown source revision is development-only evidence, not a qualified build claim |
| Native executable selection | Component-owned `P_<TOOL>_BIN` | Existing tool default; shared scalar/path validation, owner-local tool semantics |

The file-state and execution controls apply to the actual owning runtime, not merely to every process sharing its distribution. The `stove0-server` administration/scheduler commands retain their own command structure and validated database configuration. Review0 can regulate its own local work but cannot hold a child's resource while waiting for the child.

Converge observer `_STATE` to `_STATE_ROOT`. Preserve the already consistent `_WORKSPACE` spelling. Move independently owned `STOVE0_FFMPEG_BIN`, `STOVE0_FFPROBE_BIN`, `STOVE0_EXIFTOOL_BIN`, and target `_ZSTD` wiring to the relevant component tool binding; use explicit Compose values when paired executables deliberately select the same tool. Move global target/observer terminal-retention bindings to the per-component bindings backed by one shared definition/default. Remove retired spellings together with old writers, tests and generated reference; no dual-read compatibility path. This is a selected pre-v1 configuration change, not an assertion that the audited source already implements it.

## 4. Execution conventions

### C14 — Admission is component-local and real

All supplied observer, transform/effect target and sampler execution owners expose the same optional admission facility at their actual execution boundary, including metadata-only observer computation. Absent configuration preserves immediate policy. A busy probe returns without a payload worker, native process, execution session or input-content verification. No process may consume a payload slot while merely waiting for a resource. Pending metadata/state and an already assigned shared workspace are not themselves payload consumers; count actual occupancy. Support-library injection alone is insufficient: qualify the real parser/configuration through to execution.

### C15 — Cooperative resource interoperability

Use the existing FileExecutionAdmission seam, not a core GPU service or a new public scheduler protocol. Cooperating host/container participants open the same retained local regular-file inode; neither unlink nor replace it. Validate selected paths and permissions at startup, refuse symlink/nonregular targets and unavailable/invalid configured admissions rather than silently falling back to immediate execution. Use a checked host bind mount, preserve secret-file group access, and compare the actual open descriptor's device/inode across the boundary. Distinct resources have distinct domains; no global Stove0 lock. This is local cooperative Linux/POSIX behavior for the supplied Linux payload images, not distributed locking, enforced fairness or protection from nonparticipants.

### C16 — Consumption lifetime determines release

Native payloads run through the common supervised-consumer mechanism with an explicit owner scope. A wrapper import that falls back to raw Popen outside that scope does not establish containment. Cancellation, shutdown, permission loss and parent death must stop/reap the owned process group/descendants before resource or state ownership can be released. Retain descriptors in the containment supervisor where required. An explicit unlock on a duplicate descriptor can release the shared flock: inherited descriptors do not make early unlock safe. Test real process death and a child ignoring graceful termination. Record finite cancellation/escalation deadlines; inability to establish stop remains visible and fails closed rather than falsely releasing.

### C17 — Durable acceptance and restart

Persist exact nonsecret accepted job identity before acknowledgment; distinguish semantic request, caller/claim authority, execution attempt, process owner and admission probe. Duplicate exact submission converges; conflicting data under one identity is rejected. Cancellation-before-start survives delayed submissions and restart. After owner death, unstarted jobs stay dormant until current authority is refreshed; started read-only/recomputable work resumes or retries only through an explicit safe transition. Never turn an uncertain external effect into replayable work. The owning protocol determines completed-evidence precedence and replay; common machinery must not collapse those rules.

### C18 — Truthful progress, cleanup and retention

Report waiting, actual execution, publication and terminal completion accurately through the existing owning status/progress surface. Counters mean defined work units, not poll count, file appearance or status revisions. Unknown totals remain unknown; phase-local counters may reset only with a documented phase transition. Runtime limits start at the work they bound, not at an indefinite admission wait. Clean disposable terminal records incrementally and as complete owned record sets; retain pending, active, unresolved-effect and required completion/publication evidence. Retention is not authority to delete user content. Shutdown stops admission before draining/containing consumers, then releases resources and clients.

## 5. Role-specific conventions

### C19 — Observers and their semantic providers

Every observer advertises exact supported contract/interface identities and requires the matching semantic validators. Evidence binds exact subjects, request and contract; complete empty findings differ from missing/inapplicable/failed evidence. Native observers access bytes only through current read authority. The provenance observer preserves documentary scope and support. Filename-prefix comparison consumes accepted locator evidence rather than filesystem guesses or new payload reads. Batch/concurrency hints affect execution only; preserve ordering, complete logical questions and exact accepted facts. Cover all seven registered semantic-provider packages and their positive/negative vectors without importing native runtimes into them.

### C20 — Collection-producing targets and paired media roles

Opus, NVENC and Review0 return only fully validated collection outcomes with exact inputs, output roles, source edges, dispositions and publication/settlement evidence. Scratch output is not custody or completion. Preserve current materialization decisions and accepted metadata rather than guessing paths. Encoder and sampler siblings use the same owning portable media-intent contract, while full archival transformation and representative sampling remain different operations/results. No byte-for-byte equality requirement across different encoders, source versions or nondeterministic native runs. Reuse neutral mechanics, not private sibling implementations.

### C21 — Workflow external effects

Rclone remains an external-effect target over the exact selected artifacts. Bind selected destination identity and naming rules, preserve verification/manifest/readback semantics and the declared receipt. A receipt, not a fabricated collection, is the result. A potentially committed failed/interrupted attempt requires its existing reconciliation semantics and cannot be retried automatically. Source retirement follows the selected operation's explicit permission and verified outcome; lease release or sampler success confers no retirement authority.

### C22 — Artifact-free departure delivery

Retain the separate departure descriptor/intent/receipt and client/support state machine. Exercise durable admission, cancellation, restart and uncertain-commit rules with the existing support fixtures. Do not add artifact inputs, transform receipts or a new deployed departure provider merely to make a parity table symmetrical. Existing public library/tool surfaces remain in coverage even without a shipped daemon.

### C23 — Sampler-owned asynchronous jobs

Replace the synchronous terminal `POST /v1/sample` path in both supplied samplers with bounded accept/refresh, status/result and cancellation controls under the Review0 sampler contract. Do this as one pre-v1 cut across protocol, client, support, both executables, conformance/schema tools and Review0. Do not keep the old synchronous route or a second runtime. The native `sample(request)` algorithm may remain an internal worker callback; it must not be the HTTP control call.

Keep the sampler descriptor and terminal SamplerResult meaning: exact input/window/portable-intent/output-limit binding, validated source hashes and result identities, and no Riverhog authority. Add a sampler-owned nonsecret durable job envelope/status around that result rather than adopting target production/runtime capabilities. Use exact caller/job/attempt authority and current workspace assignment; neither a request digest nor service token alone proves a stale attempt may continue. Define bounded caller-owner freshness/renewal and cancellation acknowledgment so a vanished Review0 process cannot leave an orphan sampler writing indefinitely. Restart cannot trust or replay stale shared paths without current assignment and input verification after admission. Job-state durability is operational safety, not archive custody.

Keep admission delay separate from native execution timeout. Existing explicit output budgets and window semantics remain genuine request requirements. Move transport/control coordinates out of semantic commitments only by explicit owning-contract changes; do not assert that the historical SamplerRequest digest is already pure semantic identity. Exact type names and route spellings are integration choices; the lifecycle, identity distinctions and complete cut are selected requirements.

### C24 — Review0 delegates without retaining a blocked payload worker

Review0's own work must become resumable across sampler acceptance, waiting, completion validation and publication. A bounded sampler API alone is insufficient if a Review0 target worker still sits in a polling loop for hours. Persist the child binding/continuation and relinquish local payload capacity while waiting; maintain claims and cancellation independently. Resume only the exact selected sampler/job/input/descriptor binding. Never acquire the same exclusive GPU lease in Review0 and its child. If local Review0 work has an independently configured resource, release it before waiting for a child that needs it.

Keep caller lifetime/ownership distinct from resource permission. On timeout, cancellation or parent death, do not delete/reassign shared workspace while a child might still write. Require child terminal/containment evidence or an independently established containment boundary. The existing cancellation-file watcher alone cannot establish that after parent death. Keep result checkpoints and final publication with Review0; a sampler's temporary bytes and terminal result do not establish Riverhog custody. Prove two independent reviews can progress when one sampler/resource is held.

### C25 — Pure planners, compiler, contracts and validators

Keep one recipe compiler/derived callable contract and deterministic authoring semantics; no second interpreter in clients, tests or Review0. Retain explicit exact evidence, operations, branches/joins and selected output semantics. Pure validators/planners may have side-effect-free schema/conformance CLIs, but no listener, secret, lease or operational job state. Common scalar/canonicalization definitions and semantic vectors are shared only through their owning libraries. Adjacent recipe-discovery work retains its owning issue; this convention does not duplicate it.

## 6. Deployment, enforcement and completion

### C26 — Artifacts and deployment follow ownership

Keep current role/image distinctions, immutable descriptor/image pins, entrypoint discovery and independent credentials. Paired native target/sampler images may share build tooling but not service authority. Shipped Linux services remain nonroot, read-only with dropped capabilities, private networks and explicit writable state/scratch mounts. Preserve separately mounted secrets, required group access and safe explicit local-HTTP opt-in. New sampler operational state requires its own declared durable owner/mount; shared payload scratch remains ephemeral. Update the existing workspace-only sampler topology test deliberately, preserving its no-Riverhog-authority assertions. The host lease overlay must cover both NVENC consumers with the same inode, without mounting unrelated target state into a sampler.

### C27 — Executable enforcement and honest evidence

Put repository conventions in named policy tests; use real parser/configuration, app bindings and runtime boundaries for behavioral claims. Shared fixtures must cover every applicable discovered implementation, including new ones; tests of a generic base class alone do not prove executable wiring. Use real locks/processes for the portable containment witness and actual built-image/host mounts for deployment sharing. Hardware stubs may guard native entrypoints to establish control behavior; they cannot claim a real encode. Preserve existing tests and actual pytest/non-pytest required gates.

Classify expectations by their own authority under the evidence work: product behavior, repository policy, internal invariant and tooling correctness are different purposes, not different requiredness. Mixed-purpose tests are allowed. No blanket ban on mocks, per-assertion annotation mandate, new universal runner, second normative registry or test-directory migration. Agreement can prove parity but cannot prove correctness against an independently owned stronger expectation. Challenge omission, ignored config, detached native launch, late launch after cancel, early unlock, stale identity and incorrect result acceptance.

### C28 — One finite pre-v1 convergence

Implement through the current normal rail in coherent changes, not as an alternate reference runtime. Update affected current baselines, parsers, schemas, consumers, installed commands and compositions together. Remove retired forms rather than add aliases, dual formats, migration tracks or speculative version bumps. Hard cut does not permit deleting durable user data, changing protected authority without review, or treating a regenerated baseline as approval. Record a specific protected-boundary conflict when found.

Close only after the issue's complete role/convention matrix and current findings are corrected and qualified on the final source, including the original shared NVENC lease scenario. An inventory, proposed tests, one successful sampler, or a green generic support suite is not completion. Do not absorb all Riverhog configuration, evidence-accounting, growth research, recipe discovery, release qualification or provider deployment into this issue.
