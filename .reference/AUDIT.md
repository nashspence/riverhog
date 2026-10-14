# Source audit and finite subject inventory

Audited source: `6e8f93b33ea09439ad7c1e94f1907e2aea3a0f43`, tree `33d3d94721495fe1905dd844f03b254dcdc4e407`. Links below resolve to unchanged product paths in this reference tree. These are source observations and design judgments, not claims that the product suite was run.

## Authority consulted

The [developer guide](../AGENTS.md), [workspace boundary tests](../tests/unit/test_workspace_boundaries.py), [release declarations](../release.toml), actual protocols and implementations were read. Relevant owning issues were #903 (current reference process), #900 (accepted structured configuration), #948 (completed execution/control boundary), #953/#966 (compiler and discovery), #492 (configuration connectivity), #968 (test-purpose/evidence accounting), #469 (whole-contract qualification), and #434 (v1 convergence). Current #903 expressly does not require or permit native Development-link workarounds.

A full repository snapshot was used, rather than extrapolating the NVENC implementation to all roles. The read-only reference scanner reconciles the current scoped workspace projects with release-role declarations and resolves every console target to a source module. Semantic classifications below additionally use actual descriptor/protocol/result behavior; the scanner does not infer them.

## Complete current distribution inventory

There are **38 distributions**, **20 console scripts**, and **10 runtime images** in the scoped source. This is a captured inventory, not a permanent production allowlist.

| Group and actual authority | Distributions |
| --- | --- |
| Coordination/operator | `stove0-server`, `stove0-api-client`, `a-stove0-cli` |
| Observer implementations | `a-stove0-exiftool-observer`, `a-stove0-ffprobe-observer`, `a-stove0-magic-observer`, `a-stove0-filename-prefix-sidecar-observer`, `a-stove0-riverhog-provenance-observer` |
| Observer semantic providers | `a-stove0-media-metadata-contract-lib`, `a-stove0-media-sampling-contract-lib`, `a-stove0-ffprobe-streams-contract-lib`, `a-stove0-filename-prefix-sidecar-evidence-contract-lib`, `a-stove0-magic-facts-contract-lib`, `a-stove0-materialization-hint-evidence-contract-lib`, `a-stove0-riverhog-provenance-evidence-contract-lib` |
| Generic workflow/compiler/extension mechanics | `stove0-protocol`, `stove0-operator-contracts`, `stove0-recipe-config`, `stove0-extension-support` |
| Observer role layers | `stove0-observer-protocol`, `stove0-observer-client`, `stove0-observer-support` |
| Target role layers, including artifact-free departure | `stove0-target-protocol`, `stove0-target-client`, `stove0-target-support` |
| Direct transform and workflow-effect implementations | `a-stove0-opus-target`, `a-stove0-nvenc-av1-opus-target`, `a-stove0-rclone-target` |
| Media contract and mechanics | `a-stove0-media-archive-contract-lib`, `a-stove0-media-archive-lib` |
| Review application and pure authoring | `review0`, `review0-contracts`, `review0-planner` |
| Sampler role layers | `review0-sampler-protocol`, `review0-sampler-client`, `review0-sampler-lib` |
| Sampler implementations | `a-review0-opus-sampler`, `a-review0-nvenc-av1-opus-sampler` |

The 20 executable names are `stove0`, `stove0-server`; the five `a-stove0-*-observer` commands above; `a-stove0-opus-target`, `a-stove0-nvenc-av1-opus-target`, `a-stove0-rclone-target`, `review0`; the two `a-review0-*-sampler` commands; `stove0-observer-conformance`, `stove0-observer-schemas`, `stove0-target-conformance`, `stove0-target-schemas`, `review0-sampler-conformance`, `review0-sampler-schemas`, and `review0-planner`.

The runtime images are `stove0`; the five observer images; `a-stove0-opus-target` and `a-stove0-nvenc-av1-opus-target` (each includes its independent sampler executable); `review0`; and `a-stove0-rclone-target`. [Compose](../some-implementations/stove0/application/compose.yaml) contains 15 services. Do not equate 10 images, 15 service processes or 20 executables with the number of protocol implementations.

There is no independently deployed artifact-free departure implementation in the audited subtree. The protocol, support runtime, client and their fixtures are nevertheless supplied surfaces and remain in scope. External shared packages such as HTTP contracts, canonical JSON, configuration validation, Riverhog client/protocol and state machinery are dependencies to reuse or narrowly correct, not an instruction to audit every Riverhog integration.

## Confirmed source differences

### F01 — Admission exists below most executable boundaries

[NVENC app](../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/app.py) exposes `--execution-lease-file` and its component environment binding. The source inventory finds that flag in exactly one of the 20 console entrypoint modules. [Opus](../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/target.py) and [Rclone](../some-implementations/stove0/targets/rclone/src/a_stove0_rclone_target/target.py) already accept an `execution_admission` constructor argument; their apps do not connect it. [PersistentObserverService](../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/persistent.py) has the same seam, but the five app factories do not select it. This establishes missing conventional wiring, not a demonstrated malfunction in immediate mode.

### F02 — Both samplers use synchronous terminal delivery and raw native launch

[SamplerHttpBinding](../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/http_binding.py) blocks on a BoundedSemaphore and calls `sample()` inside the request. Both sampler [NVENC](../some-implementations/stove0/review0/samplers/nvenc-av1-opus/src/a_review0_nvenc_av1_opus_sampler/common.py) and [Opus](../some-implementations/stove0/review0/samplers/opus/src/a_review0_opus_sampler/common.py) helpers use ordinary subprocess.Popen and local terminate/kill handling, rather than the component consumer scope/guardian. Their app factories do not establish a corresponding service shutdown lifetime. Adding flock inside `sample()` would not establish dormant payload capacity, parent-death containment or bounded HTTP control.

### F03 — Parent-side waiting and cancellation also need correction

[ReviewMaterializeTargetService._sample](../some-implementations/stove0/review0/application/src/review0/target.py) synchronously calls the sampler client and starts a cancellation-file watcher. [ReviewSamplerClient](../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/client.py) has a default 86,400-second HTTP timeout; the descriptor fetch uses that same client. The enclosing target execution retains a payload slot while sampling. Source inspection supports the blocked-capacity concern. Parent-process death, timeout/workspace reuse and child cleanup are required new runtime challenges; this audit did not execute those failure scenarios.

### F04 — Review0 bypasses the common bounded body reader

[review0/http.py:create_app](../some-implementations/stove0/review0/application/src/review0/http.py) calls `await request.body()` before its target binding. Maintained native targets and observers instead use `read_control_body(...maximum_request_bytes=...)` and typed staging errors. A later binding limit does not bound the earlier body materialization. Add a stream/chunked-body regression through the real app and retain complete large logical metadata transport.

### F05 — Common role HTTP tests omit three supplied observers

[_application_set](../tests/unit/test_role_service_openapi.py) includes ExifTool and FFprobe, but not magic, filename-prefix sidecars or Riverhog provenance. Existing separate tests may cover some behavior; this is specifically an omission from the shared role parity fixture. The packet's test-only patch adds those three actual app factories with real descriptors and narrowly stubbed local magic readiness. Production integration must additionally enforce discovered/exercised membership, not simply replace one manually curated list with another.

### F06 — Magic readiness can emit an undeclared 503

[Magic app](../some-implementations/stove0/observers/magic/src/a_stove0_magic_observer/app.py) catches verify errors and returns `service_unavailable`/503, but its readiness decorator does not declare a 503 response model. The proposed patch contains a direct runtime/readout regression. It is expected to fail on the audited source; the producer did not run repository tests. The exact deployed readiness/error behavior must be checked during integration.

### F07 — Configuration shape is partly conventional, partly incidental

All five observer entrypoints use component `_STATE`; native target/Review0/Rclone apps use `_STATE_ROOT`. Several independent native roles read `STOVE0_FFMPEG_BIN`; FFprobe and ExifTool use similar global spellings, while other native selections use component prefixes. Observer and target retention have separate globally named configuration helpers and matching 30-day defaults. Most simple leaf apps accept direct `_TOKEN` values as well as `_TOKEN_FILE`; Review0 and Rclone structured config use `token_file`. These are established source differences. Their convergence is a selected new repository/configuration rule, not proof that each old spelling was a bug.

### F08 — Startup/lifetime policies need real failure tests

[FileMagic and Magic app](../some-implementations/stove0/observers/magic/src/a_stove0_magic_observer/app.py) create/inspect the native engine before reading the token. Native target apps construct the target runtime before `_secret`; their cleanup relies on later ASGI lifespan. Observers have explicit final service close in main, while sampler apps lack an equivalent sampler-service lifetime. These orderings motivate invalid-token, invalid-image, server-start-failure and SIGTERM tests. Do not label every construction path a confirmed leak without executing the failing path.

### F09 — Deployment tests encode assumptions that the sampler fix must change

[Compose topology tests](../some-implementations/stove0/application/tests/test_compose_topology.py) require sampler volumes to equal only `review0-workspace:/run/review0`. [The host lease overlay](../some-implementations/stove0/targets/nvenc-av1-opus/compose.host-lease.yaml) mounts the host resource only into the NVENC target. The sampler needs the shared resource and a separately owned operational job-state mount under the selected lifecycle. Update the exact mount assertions without weakening network, credential or publication-authority isolation. Preserve secret-file supplementary group access when adding the lock group.

### F10 — Similar models do not imply identical identity semantics

[SamplerRequestPayload](../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py) currently seals workspace, timeout and cancellation-path fields into its request. [Rclone target](../some-implementations/stove0/targets/rclone/src/a_stove0_rclone_target/target.py) deliberately binds destination identity and naming rules in its plan checks. Therefore a universal test asserting all deployment/configuration changes preserve all request/result hashes would be wrong. The sampler hard cut must explicitly separate job transport/authority from semantic data without stripping meaningful selected limits or effect commitments. Review exact-scalar ownership rather than assuming all similarly named numeric fields already share a type.

## Existing mechanisms worth retaining

[Extension support](../some-implementations/stove0/packages/extension-support/src/stove0_extension_support/) already separates ExecutionOwner/Admission/Permit, a bounded dispatcher, terminal retention, native consumer scopes and a guardian. Its file admission uses a nonblocking kernel lock and refuses unsafe selected files. [File-admission tests](../some-implementations/stove0/packages/extension-support/tests/test_file_admission.py) include a hard parent kill with a stubborn child. [NVENC role tests](../some-implementations/stove0/targets/nvenc-av1-opus/target/tests/test_nvenc_av1_opus_roles.py) exercise actual argument/environment wiring. [The host witness](../scripts/test_nvenc_host_lease.py) qualifies the maintained image's guarded native boundary with a real shared inode, not real GPU encoding. Preserve and extend these rather than replace them with a new broker or fake-only suite.

[Target persistent support](../some-implementations/stove0/packages/target-support/src/stove0_target_support/persistent.py) preserves uncertain effects as interrupted and retains successful effect receipts. [Departure support](../some-implementations/stove0/packages/target-support/src/stove0_target_support/departure.py) is separately typed. [Observer result models](../some-implementations/stove0/packages/protocol/src/stove0_protocol/models.py) distinguish terminal delivery from observed/inapplicable/failed/canceled results. These differences are semantic protections, not parity defects.

## External mechanism cross-check

The Linux [flock manual](https://man7.org/linux/man-pages/man2/flock.2.html) confirms open-file-description ownership, nonblocking contention and release by explicit unlock or closure of all duplicates. The Python [subprocess documentation](https://docs.python.org/3/library/subprocess.html) documents descriptor passing/session creation and the need for cleanup after timed communication. These support the containment review; they do not prove this repository's crash safety. No external source defines Riverhog's ontology or acceptance.

## Limits

This is broad source/contract/policy research, not exhaustive path-by-path product certification. No real FFmpeg/Rclone run, Docker topology launch, HTTP failure test, repository pytest gate, database/restart qualification or GPU/provider run was performed by the producer. The reference-reader tests establish the reader/coverage guard only. The integration acceptance requires actual maintained behavior and final-source gates.
