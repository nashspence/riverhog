# Post-v1 integration map for #959

The owning issue controls. This document supplies implementation seams and a
bounded sequence, not a pre-v1 work order. Research is in `RESEARCH.md`; constants
and pure examples are in `CONTRACT.json` and `ledger_reference.py`.

## 1. What to reuse and what to replace

At old reference `bcabb02bef4d12d45301fcf0419c850ea0e96e06`:

- Reuse/reconcile public-API pagination, explicit coverage failures, source-bound
  literal validation, credential-neutral diagnostics, canonical records and age
  sealing from `scripts/downstream_reuse_watch.py`.
- Keep `scripts/downstream_reuse_signatures.json` as optional reviewed public
  discovery canaries, never a claim of rename-resistant/global clone detection.
- Retain useful focused tests, but replace assumptions about one observation-only
  artifact, private-host state and a single collection/attestation job.
- Replace the old `.github/workflows/downstream-reuse-watch.yml`. The new workflow
  must separate candidate parsing from OIDC/cloud publication authority.

At audited main `a26e214f77ab806e4230125bf7aad93f8c3328da`, #954 has reshaped
qualification. Reconcile `scripts/ci_qualification.py`, ordinary test discovery,
`tests/unit/test_github_actions.py`, `pyproject.toml` support typing and Make targets.
Do not revert that work or add the live watch to `CI gate`/`release.toml`.

Suggested ownership, to reconcile against post-v1 source:

| Owner | Responsibility |
| --- | --- |
| `scripts/downstream_reuse_watch.py` | Thin scheduled collection/coordinator CLI. |
| Focused maintenance module under `scripts/` | Pure records, acquisition ports, NiCad adapter, ciphertext transport and store client; no product imports. |
| `tests/unit/test_downstream_reuse_*.py` | Offline policy/format/error witnesses in ordinary commit qualification. |
| Focused integration fixtures | Real pinned NiCad, age and disposable PostgreSQL qualification; synthetic cloud canary separately authorized. |
| `.github/workflows/downstream-reuse-watch.yml` | Independent opt-in analysis and fresh publisher jobs. |
| `infra/reuse-watch/` | One IaC definition, SQL migrations/roles, managed importer/query deployment and operational checks. |

Use an existing IaC convention if one exists at implementation time; otherwise
choose one small maintained definition, not both Terraform and CloudFormation.
Cloud account IDs, keys, ARNs, endpoints and deployment state are not source.
This is project-maintenance tooling, not a new Riverhog product/database/service.
The managed application nevertheless owns its own schema and migration history.

## 2. Three deliberately different identities

**Acquisition identity:** exact provider/repository ID, observed Git commit/tree/blob
or explicitly bounded snapshot manifest, UTC acquisition timestamp, retrieval method
and selected file byte digests. Keep Git's object identity distinct from raw-file
SHA-256. A search blob is not proof of membership in today's default-branch commit.

**Logical work key:** canonical input identity from `CONTRACT.json`, domain-separated
as `riverhog-reuse-work/v1`. Candidate/upstream corpus manifests bind the exact
selected paths and bytes; snapshot evidence binds commit/tree/blob context and scope.
The license manifest binds historical REUSE/license/notices. Toolchain includes
NiCad, OpenTxl, image and parser identities. Profiles/extraction policy include
normalization, thresholds, size limits, language and exclusion rules. Do not dedup
against current filenames, timestamps, ciphertext hashes or only a candidate commit.

**Custody identity:** SHA-256 and length of actual age ciphertext plus exact
producer/publisher workflow/run/attempt and attestation. Re-encryption changes this
identity even when analysis inputs match. Retain multiple attempts/evidence objects
where appropriate. Do not assert deterministic ciphertext or replace an old signed
object with re-encrypted bytes.

## 3. Acquisition and NiCad adapter

Discovery uses public APIs and records each query's coverage, not a universal search
claim. Carry a bounded pending-candidate set in hosted state when a run's budget is
exhausted; initial implementations may simply record deferred work and repeat scans.
Do not silently drop candidates or treat every repeat as newly discovered.

The initial escalation rule is a qualified literal hit or explicit maintainer
selection; forks alone are inventory. This rule selects a comparison, not a suspect.
Compare only a reviewed CAL corpus, respecting file exceptions and third-party
provenance. A short string can appear in an independent compatible implementation.

Resolve fixed snapshots before analysis. Fetch only public HTTPS provider origins,
validate every redirect/size budget and strip credentials at origin changes.
Preserve source bytes without imports/builds/hooks/submodules. Safe staging names
must retain an original-path mapping; reject escaping paths, links and archive bombs.
A subset is permitted only with an exact selection manifest and explicit coverage.
Missing, unsupported or over-budget source is not a negative clone result.

Build a pinned NiCad/OpenTxl image before introducing candidate data. The audited
NiCad source is `7a90d11795a7fe585282e25b7fa8d9964f202965`; it requires OpenTxl 11+
and `make`. Revalidate and pin all actual dependencies at post-v1 integration; this
reference has NOT built them. The upstream config lives in `src/config` in source;
verify the installed layout rather than assuming the source layout is executable.

Materialize the three reviewed profiles from `CONTRACT.json` in trusted tool config.
The native command shape, with locally assigned safe paths/config names, is:

```sh
./bin/nicadcross functions python upstream-corpus candidate-corpus profile-name
```

No parameter above comes directly from candidate text. Run in a non-root,
network-disabled container with read-only tooling/source, bounded writable scratch,
CPU/memory/PID/time limits, no secrets and no Docker socket. Tool reports are
untrusted parser output. Do not render raw HTML in a privileged browser or execute
embedded links. Retain raw reports, independently acquired originals, exact mapped
ranges and normalization settings; render a separately escaped comparison view.

The initial profiles are normalized exact (`rename=none`, threshold 0), blind exact
(`rename=blind`, threshold 0) and blind near-miss (0.20). They share explicit
minsize=10, maxsize=2500 and no extra transform/filter/abstraction/normalizer.
Those size limits are operational coverage exclusions, NOT legal significance
thresholds. NiCad's numbers concern pretty-printed lines, not infringement odds.

Test actual current Python syntax, scope-sensitive identifier renames, comments,
formatting, small edits, unrelated boilerplate and complete parser failure. Record
extraction totals and excluded/failed inputs; process exit 0 alone does not prove
coverage. No bespoke AST/ML expansion or claimed legal-admissibility threshold.

## 4. Workflow and the ciphertext-to-index seam

Use one scheduled/manual workflow, upstream/default-branch allowlist and explicit
enable variable. Off-hour cron and an external freshness check account for delayed,
dropped or inactivity-disabled schedules. No PR-controlled privileged trigger.

**Analysis job:** read-only public discovery credential only, no `id-token: write`,
no cloud/DB/decryption credentials, and no candidate code execution. Its sandbox
produces bounded evidence; a trusted wrapper on that disposable runner encrypts it
to the configured managed-service age recipient. Upload only fixed neutral-named
ciphertext. No candidate-bearing matrix/job names, stdout, summaries, caches,
public outputs or plaintext metadata sidecars. Run size/timing remains observable.

**Fresh publisher job:** checks out trusted code at the approved coordinator SHA,
resolves the exact analysis-job artifact by repository/run/attempt and neutral name,
validates the bounded transport without arbitrary extraction, then attests the
ciphertext and stages it using GitHub OIDC -> scoped AWS role -> Data API. Preserve
the attestation bundle/trusted run metadata, not just their URLs. It never decrypts,
parses NiCad reports or executes artifact-provided code. A valid attestation records
workflow provenance; it does not establish the factual correctness of its input.

**Managed importer:** a small Lambda deployment of the custody application reads the
staged ciphertext, verifies expected workflow/source/run/attempt and byte identity,
then uses a dedicated age identity from managed secret storage. Authenticate the
whole plaintext before publishing/indexing it. Validate the bounded manifest,
original-byte hashes and record references, then commit private index rows. It does
not run NiCad, import source, browse report HTML or obey evidence text. Missing or
failed indexing is visible and retried independently of ChatGPT.

This extra managed step is necessary for the selected confidentiality boundary:
a publisher that only has ciphertext cannot populate candidate/clone SQL fields.
Do not leak those fields through public artifacts to avoid implementing the step.
One maintained application can supply importer and read adapters; keep execution
roles separate rather than creating a general-purpose service platform.

Initially tolerate repeated comparisons instead of giving the analysis job DB
credentials for an optimization. Later exact-work lookups may use a separately
authorized bounded query operation; a cache hit is valid only for retained,
verified, fully covered work. Partial/failed/stale/missing results cannot suppress
comparison. No success watermark is inferred from an artifact's mere existence.

## 5. Relational ownership and Data API protocol

Minimal relational families:

| Relation | Key/meaning |
| --- | --- |
| `scan_run` / `scan_event` | Source repo ID + workflow/run/attempt; append operational events and per-query coverage. |
| `candidate` | Provider + numeric repository ID; names are mutable attributes, not identity. |
| `observation` | Stable observation ID + scan link + source evidence pointer. |
| `analysis_work` | Exact logical work key and immutable input tuple. |
| `analysis_attempt` | Work key + run/attempt; result/coverage and admitted evidence reference. |
| `clone_pair` | Analysis attempt + original source/range pair + profile, never a legal verdict. |
| `evidence` / `evidence_chunk` | Ciphertext digest/length/ordinal; immutable after custody validation. |
| `ingestion_event` | Staged/custodied/indexed/rejected transitions and retry diagnostics. |
| `review_event` | Separately authorized principal/model, exact analysis/evidence, time and interpretation. |

Implement foreign keys, exact uniqueness, bounded fields and grants in normal SQL.
Use one owner/migration history. Ordinary publisher cannot UPDATE/DELETE admitted
facts, write review state or read arbitrary private candidate rows. Indexer writes
only admitted projections; reviewer reads approved views/evidence. Migrator/admin
has separate credentials. Database-owner power remains a trust limitation, not WORM.

Store evidence as 16 KiB ciphertext chunks with whole-object and per-chunk hashes.
A small Data API page/batch of eight chunks leaves room for base64/JSON/metadata.
The sample `encoded_batch` checks its rows only; the real adapter must measure the
FULL serialized AWS request/response including wrappers. No unbounded `SELECT *`
over source/report/payload data. Large metadata/reports are evidence members,
not giant returned rows. Keep native SQL types supported or explicitly cast them.

Protocol:

1. `begin`: supply fixed run tuple, neutral evidence ID, expected digest/length,
   chunk count and provenance identity. Matching retry is idempotent; conflicts fail.
2. `put_chunk`: ordinal, bounded bytes and hash; exact duplicate is idempotent,
   conflicting duplicate fails. A DB uniqueness constraint is mandatory.
3. `custody_finalize`: verify complete ordinal coverage, lengths and aggregate
   ciphertext hash; retain provenance and acquisition binding. Staged rows are not
   completed analyses. Treat an uncertain network commit as unknown and resolve by
   identity, not by blindly inserting another completion.
4. `verify_and_index`: importer authenticates, decrypts/validates and commits
   relational projections plus a final admitted state in one bounded transaction.
   External network/crypto/analysis work happens before that transaction. Separate
   artifact custody, verified indexing, analysis result and human review states.
5. `read/export`: keyset-page admitted indexes and chunk rows; reassemble and verify
   every evidence object. Preserve explicit history gaps and partial observations.

Data API has 64 KB per returned row, 1 MiB per response, 4 MiB per request and a
three-minute transaction-idle timeout. Serialize statements within a transaction;
use bounded retries and idempotent recovery. Never keep a transaction open while
NiCad executes. No response-limit failure becomes an empty successful read.

`ledger_reference.py` is a PURE specimen for identities/chunks/reuse only. It does
not supply the SQL above, concurrent finalization, transport auth or cloud retries.
Those require real PostgreSQL and AWS adapter tests before integration acceptance.

## 6. Deployment, cost and handoff

Preferred evaluated AWS profile: one Aurora PostgreSQL `db.serverless` writer,
Standard storage, min 0 ACUs, an explicit modest maximum and 300-second idle pause;
Data API enabled, no public DB port, no RDS Proxy, no NAT gateway dependency and no
always-on connection pool. Confirm region/version support rather than hardcoding
an old engine minimum. Use short-lived OIDC for the publisher, separate Secrets
Manager SQL credentials, and a separate managed age secret accessible only to
importer/reader roles. A hosted MCP endpoint never exposes keys to the model.

Use actual repository OIDC subject/audience values and approved environment/ref
restrictions. New immutable-ID subject formats and a transfer can change bindings.
A default-branch-name check in YAML alone is not an IAM trust restriction.

Before apply, record current regional compute/storage/I/O/backup/secret/key/service/
network/logging charges, normal and failed-to-pause scenarios, retention/growth
assumptions and a maintainer-approved spending limit. Do not assume a near-zero bill.
Aurora documentation describes a normal 10 GiB storage starting allocation, while
current new-account promotional limits differ: verify actual billable allocation
and do not turn introductory credits into the operating cost model. Check pause
metrics after an idle cycle; retries must handle waking/deep-sleep instances.
Budget alarms are warnings, not automatic spending caps.

The repository owns IaC, application/schema versions, private configuration schema,
secret bootstrap references, rotation/recovery, scheduled stale-run checks and restore
procedures. Keep actual deployment state and secrets in managed custody, not Git or
the original maintainer's only laptop. No continual DB health polling just to wake it.
A scheduled managed freshness function can first inspect GitHub metadata and alert
on missing runs without querying PostgreSQL on every check.

Export includes manifests, immutable ciphertext/chunks, provenance, relational facts,
review events and schema version. A logical PostgreSQL restore requires ordinary
SQL access; Data API is not a transparent `pg_dump` connection. Supply either bounded
application export/import or an explicitly provisioned temporary managed network
executor for native PostgreSQL backup tools. Do not introduce a hidden home VPN
or permanent bastion. Verify restoration, byte fixity and historical decryption.

Test a fresh project/account with the former maintainer's machines offline. Recreate
infra, import records, securely transfer/restore age identities, verify old evidence,
rotate bindings/read credentials and produce a new synthetic run before revocation.
Cloud account/billing, resource access and legal rights are separate from repo
ownership. Re-encryption keeps old signed ciphertext and records the transformation.
Object storage is optional later for measured payload pressure, not the case index.

## 7. Ordered delivery and honest stopping point

After post-v1 prioritization: (1) reconcile source and costs, (2) qualify acquisition
and real NiCad, (3) implement/qualify SQL and bounded transport, (4) isolate jobs and
implement managed importer/read/export/IaC, (5) run repository gates, (6) perform an
explicitly authorized synthetic cloud/restore/handoff drill. Leave real monitoring
disabled until operator configuration and costs are accepted.

The read-only MCP adapter exposes `list_runs`, `list_candidates`, `get_analysis`,
`get_evidence` with authorized opaque IDs/cursors, bounded results and evidence
citations. No SQL/shell/arbitrary path/URL/decrypt capability or automatic notices.
Account/OAuth/task support must be tested; interactive export remains available.
No model writes or inferred human-review completion are necessary initially.

This reference hands off a researched design and pure witnesses, not deployable
production machinery. Existing and new tests do not certify legal admissibility.
Do not mark #959 complete from this branch or charge cloud resources to prove a
reference. Later actual implementation must record its own exact-SHA validation.
