# Integration map for #959

This is concrete input under #903, not an alternate authority. The owning issue
controls scope. Earlier conversational sketches are deliberately not imported as
requirements.

## Proposed file ownership

| File | Integration purpose |
| --- | --- |
| `scripts/downstream_reuse_watch.py` | Public GitHub collector, historical catalogue validation, age sealing, strict offline envelope and transport-ZIP validators. No product implementation imports. |
| `scripts/downstream_reuse_signatures.json` | Three reviewed literal origins, exact source commit/file SHA-256 and REUSE SHA-256. This is query policy, not a list of suspected parties. |
| `.github/workflows/downstream-reuse-watch.yml` | Separate weekly/manual opt-in operation; ciphertext attestation followed by ciphertext upload. |
| `tests/unit/test_downstream_reuse_watch.py` | Synthetic behavioral/security witnesses plus a real age test requiring the installed tools. |
| `tests/unit/test_downstream_reuse_workflow.py` | Narrow executable workflow policy. Existing `test_github_actions.py` remains unchanged. |

All new implementation/test files fall under the existing default Apache-2.0
REUSE rule. No component license changes are proposed. Do not copy a second license
resolver into the scanner. The source catalogue's origins deliberately use the
actual `REUSE.toml`, not the less detailed top-level license summary.

## Integrator sequence

1. Compare the exact reference commit with its audited base and current main.
   Preserve current #954 CI work. Import/reconcile the five files above at their
   existing tooling owners; do not merge `.reference` as product documentation or
   introduce a package/application/archive database. Add the new script to the
   current explicit mypy support-path selection in `pyproject.toml`; do not disable
   strict checks. The tests are already in ordinary `tests/unit` discovery.
2. Run the repository's formatter, Ruff, mypy and REUSE checks under its locked
   toolchain, then the focused tests below. Correct any toolchain-specific issues.
   The external environment could not qualify those tools; see HANDOFF.md.
3. Run actual age/age-keygen fixtures with ephemeral synthetic identities. Confirm
   the native test executes rather than skips. Never commit its generated keys.
4. Run `make lint`, `make unit`, `make dist-smoke`, `make build` and then-current
   additional gates. Follow the current authoritative integration rail and observe
   exact-pushed-SHA GitHub checks. The live watch itself must not become a required
   CI/release check. No changes to `release.toml`, main protection or provider gates
   are required by this feature.
5. Record machinery qualification on #959. Leave `REUSE_WATCH_ENABLED` unset until
   the separately operated activation checks below are complete. No maintainer
   production key, host URL or credential belongs in the commit.

Suggested focused invocation in the repository environment:

```sh
mise x python age uv -- uv run --locked --all-packages --group dev \
  python -m pytest -q tests/unit/test_downstream_reuse_watch.py \
  tests/unit/test_downstream_reuse_workflow.py tests/unit/test_github_actions.py
```

## Collector behavior already represented

One snapshot contains the run tuple, UTC observation timestamp, reviewed catalogue,
per-query coverage and sorted observations. `source_commit` is the collector's
checked-out source SHA; `catalogue.source_commit` is the historical origin SHA.
They need not be equal. The entire public policy/catalogue is bound inside the
encrypted snapshot, as well as by source provenance. No self-referential manifest
or release-signature refresh is needed.

The collector inventories forks independently of search indexing. It then searches
three literal origins using REST lexical search, not the browser's regex engine.
GitHub supplies `sha` as a blob ID; this reference keeps `commit_sha: null` rather
than mislabeling it. The stored blob API URL identifies exact content; it is not
a GitHub browser URL fabricated with a blob SHA in the commit slot. Captured snippets
are evidence returned by search, not independently re-fetched whole-file proof.

Policy defaults are three pages of up to 100 items per query, 32 total HTTP calls,
a 240-second request budget, 6.2 seconds between code searches, a 20-second maximum
request timeout, 2 MiB response bound, 1,200 unique observations and an 8 MiB JSON
envelope limit. HTTP error responses are not logged. Rate-limit errors stop that
query and remain encrypted coverage gaps; there is no retry storm or HTML scraping.
The next scheduled run is a new snapshot, not an implicit continuation. Increasing
budgets or introducing resumable scans is a later explicit policy change.

`complete` means only that the configured API snapshot completed without detected
gaps. It does not mean all uses were found or licenses were satisfied. Page/result
budgets, private results, bad items, duplicate pages, result-count changes, snippet
truncation and API incompleteness remain visible. Source/private credentials are
never copied into the observation envelope. All downstream snippets stay untrusted.

Missing/invalid recipient configuration fails before collection by an actual empty
age encryption preflight. The full snapshot is held in memory, encrypted and written
exclusively to one `.age` file. There is no plaintext disk report or cache. The CLI
returns success for a sealed **partial** snapshot so that attest/upload can finish;
the final workflow step then reports an operational failure. If sealing or
attestation fails, ordinary step failure prevents artifact publication.

## Public storage and trust boundary

The only artifact member is `observations.age`; name and retention are fixed by
workflow policy. Default retention is 30 days. Logs contain operational status,
not query results/counts. The action attests the ciphertext, not plaintext hashes
or candidate metadata. OIDC signing avoids installing a long-lived signing secret
in Actions. The attestation is public provenance, not a legal certification.

The runner sees plaintext during collection. This is confidentiality from public
artifact readers, not from GitHub/the runner or a compromised approved workflow.
Traffic metadata and prior copies remain visible/retained outside our control.
For a stronger platform-confidentiality requirement, collection must move to the
private host; changing encryption algorithms does not fix that boundary.

The reference uses the audited checkout/upload/mise pins and verified
`actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6`. Its real action.yml provides
`subject-path`, `show-summary` and `bundle-path`. Only the ciphertext is uploaded;
the host separately retrieves and preserves attestation material. Do not assume
short-lived Actions artifact storage is the historical archive.

## Exact private-host interface

`MCP_TOOLS.json` contains the proposed read-only tool inputs. This is a deployment
interface specimen, not a running server or a new Riverhog runtime component.
The host implements ingestion outside model-request execution:

1. Authenticate to GitHub as needed, allow only the configured numeric repository
   identity and approved workflow path/source policy, and enumerate all relevant
   run attempts/artifacts with pagination. Do not accept arbitrary model-supplied
   URLs, repository names or artifact paths. An artifact name alone is not identity.
2. Download into bounded host-controlled storage. Limit redirect hops, strip API
   credentials on redirects, validate HTTPS and approved GitHub download origins,
   and reject loopback/private/link-local targets and DNS rebinding. Never pass a
   GitHub or MCP bearer token to the returned asset URL. Do not rely on an expired
   signed URL saved from an old response.
3. Call `ciphertext_from_artifact(raw_zip)` to obtain the single bounded member
   without writing ZIP paths. This helper is transport validation, **not** signature
   verification. The ZIP transport digest and attested `.age` digest are different
   byte identities. Reject extra files, links, malformed ZIP and oversized members.
4. Verify the ciphertext using the installed, pinned GitHub CLI/verifier and trusted
   roots. An illustrative host-owned command (not an MCP tool argument) is:

   ```sh
   gh attestation verify observations.age \
     --repo nashspence/riverhog \
     --signer-workflow nashspence/riverhog/.github/workflows/downstream-reuse-watch.yml \
     --source-ref refs/heads/main --source-digest "$APPROVED_SOURCE_SHA" \
     --deny-self-hosted-runners --format json
   ```

   Resolve `APPROVED_SOURCE_SHA` from the host's authorized source policy and the
   actual run, not a bundle's self-asserted field. Require the correct ciphertext
   subject/predicate, repository and OIDC signer identity; retain the verifier
   result and attestations. The attestation statement's predicate is workflow-
   supplied, unlike the OIDC-derived certificate. Source/workflow approval and the
   independently obtained GitHub run/attempt metadata are therefore essential.
5. Decrypt with the host's dedicated identity, no shell and no model-supplied path.
   Bound ciphertext, timeout, memory and plaintext output. Buffer until age exits
   successfully: streaming plaintext emitted before a final authentication failure
   must never reach MCP. Keys remain on the host. Never log plaintext or CLI stderr.
6. Build `expected_run` independently with exactly `repository`, `repository_id`,
   `source_commit`, `workflow`, `run_id`, `attempt`; then call
   `validate_envelope(plaintext, expected_run)`. Its success checks shape, hashes and
   bindings only. It cannot replace step 4 or establish compliance. Refuse the wrong
   run/source, unsupported format, duplicate keys or altered record identities.
7. Archive ciphertext, trusted metadata, attestations and verification identity
   durably before advancing ingestion state. Index by repository ID/run/attempt/
   ciphertext digest; allocate a host-owned opaque `run_key`. A byte-identical retry
   is idempotent. Conflicting bytes for an already admitted identity are quarantined.
   A missing/expired/deleted artifact is an explicit `history_gap`, not no findings.
8. Advance review state only after actual authorized review, separately from
   ingestion. A partial snapshot may be archived/reviewed but cannot satisfy a
   complete-observation watermark. Inspect failed runs too: the intended final
   partial-status failure is acceptable only when collection, attestation and
   upload succeeded for that exact attempt and the admitted envelope is partial.

Authenticate and authorize **every** MCP call. Cursor tokens are host-issued,
bound to principal/run/version, not arbitrary offsets or paths. `list_runs` returns
bounded admitted run descriptors plus explicit gap states. `get_observations`
returns a bounded page of actual validated records, coverage and provenance;
`get_evidence` returns one matching observation's captured excerpts and source
identity. Keep the encrypted/raw entire envelope out of model context by default.
Return the opaque run key, exact GitHub run URL, source SHA and ciphertext digest
with every response so reports can cite retained evidence. Read-only MCP annotations
are hints, not access controls. Do not implement writes, shell, generic decrypt,
generic fetch, automatic notices or automatic legal dispositions.

Treat all returned downstream content as quoted evidence, never instructions. A
model response can still disclose plaintext or follow a malicious snippet despite
read-only tools. Restrict destinations/tools and output review accordingly. Sharing
through MCP intentionally sends selected plaintext to the model provider under the
operator's account/data controls; possession of the decryption key is not required
for that disclosure.

## Operator activation, deliberately not performed here

Generate/back up a dedicated age identity on the private host and configure only
its public recipient as `REUSE_WATCH_AGE_RECIPIENT`. Qualify `GITHUB_TOKEN` for the
actual public code-search endpoint; configure a least-privileged dedicated
`REUSE_WATCH_SEARCH_TOKEN` only when needed. Do not ask for a broad classic `repo`
permission merely to read public code. Missing token capability is a coverage gap,
not an invitation to scrape or search private repositories.

Before opt-in, execute a synthetic Actions canary using the same proposed workflow
path/trust policy and ephemeral test recipient, then verify the actual downloaded
ciphertext/attestation and decryption on the host. Keep canary and production keys
separate, record the accepted source/workflow tuple and have the host ingest well
before the 30-day expiry. Provision an independent freshness check: a scheduled
workflow cannot detect that GitHub disabled it after inactivity or failed to run it.
A host scheduler should report operational staleness without publishing candidates.

Configure the authenticated remote MCP transport (or a supported secure tunnel) and
verify authorized and unauthorized requests, cursor scoping, bounded evidence and
no secret disclosure. Then test one unattended ChatGPT invocation on the actual
account/app configuration. Task support for connected apps is not proof that this
custom app's authentication works unattended. Interactive review or a host scheduler
can use the same admitted evidence when a scheduled ChatGPT route is unavailable.
Only after these operator checks should `REUSE_WATCH_ENABLED=true` activate the real
weekly watch. Nothing in this reference changes that setting.

Stop at repository machinery plus the defined consumer seam. Hosting, key custody,
OAuth enrollment, scheduling and human/legal investigation are intentionally outside
the integration issue's code changes, not hidden requirements to invent in main.
