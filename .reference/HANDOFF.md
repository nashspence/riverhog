# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#959](https://github.com/nashspence/riverhog/issues/959)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Audited base: `main` @ `f5d09bfdaef74d8729362df449ecbe0441b9c19c`
Audited base tree: `aa2cff9dfa01746f3ebc0c4c6bc3040578d7734a`
Reference branch: `reference/external/959-encrypted-reuse-watch`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, @nashspence
Published by: OpenAI ChatGPT through the connected GitHub publication tools
Prepared: 2026-10-05

This branch is externally produced reference material for #959. Its creation,
testing, publication or linkage does not establish or extend an accepted design,
contract, release requirement, or authorization to integrate. Authority remains
with the owning issue, subsequent maintainer decisions and the repository's normal
integration rail. Reconcile against then-current authoritative source, including
ongoing #954 CI work; do not apply mechanically. No main/release ref, protection,
provider setting, secret, repository variable or scheduled task is changed here.

## Contents and next action

The reference contains a concrete stdlib-only collector, a three-literal catalogue
bound to actual audited source bytes, strict offline envelope/ZIP validators, an
opt-in scheduled workflow and focused synthetic/policy tests. It does not implement
or deploy a network MCP server. `INTEGRATION.md` specifies the exact host boundary,
validation sequence and activation checklist; `MCP_TOOLS.json` supplies constrained
tool inputs. `RESEARCH.md` records primary sources and corrections to the earlier
conversation.

Start with the owning issue, then `INTEGRATION.md`. Reconcile the five proposed
production/test files with current owners, run the locked repository tooling and
normal integration gates, and land only through the current integration rail.
Retain this directory as external-reference provenance, not a new product manual.
The actual host, OAuth/account setup, production key and live token qualification
are operator deployment inputs, not secret placeholders to fill into source.

## Validation actually performed

At the prepared reference tree, in this container with Python 3.13.5:

```sh
pytest -q tests/unit/test_downstream_reuse_watch.py \
  tests/unit/test_downstream_reuse_workflow.py \
  tests/unit/test_github_actions.py
```

Result: **78 passed, 1 skipped**. This includes existing Actions policy tests and
new tests for pagination, duplicate and incomplete results, authentication/rate
failures, private-result exclusion, stable IDs, blob-versus-commit identity,
strict JSON, envelope binding, source-catalogue fixity, safe ZIP transport,
credential isolation, ciphertext-only writing, partial-publication flow, and
upstream/branch/opt-in/workflow-policy guards. Remote HTTP and encryption ports
are mocked where indicated. A mocked age header is never cryptographic proof.

`python -m py_compile scripts/downstream_reuse_watch.py` also passed. Catalogue
validation successfully used exact `git show` bytes from the audited base,
including the `REUSE.toml` digest. The three declared origins were reviewed under
the applicable CAL path rule and outside its Apache exceptions.

Publication should preserve these supplied file bytes and the audited parent.
The publisher verifies the final Git tree and records the **actual published
commit SHA** in the owning issue. That SHA, not a branch label or unpublished
local commit, is the handoff identity.

## Not validated / known limitations

- The actual age round-trip/tamper test was skipped because `age` and `age-keygen`
  are not installed here. Attempts to obtain the locked binary were unsuccessful.
  The test must run, not skip, in the repository's mise-managed environment.
- The environment is not the repository's locked Python 3.12.3 toolchain. Ruff,
  mypy, REUSE, mise and the full workspace dependencies are unavailable here;
  package installation was blocked by network DNS. Full `make lint`, `make unit`,
  `make dist-smoke` and `make build` were **not run**. Formatting/typecheck and full
  integration qualification remain explicit steps, not claimed passes.
- No live GitHub search credentials, scheduled Actions run, OIDC attestation,
  artifact download, verified host ingestion, actual MCP authentication or
  unattended ChatGPT invocation was exercised. The reference does not certify
  deployment. The publication comment records any exact-SHA workflow observations.
- Search results are bounded snapshots, not exhaustive reuse detection. They
  exclude private results, do not fetch every matching blob, and retain bounded
  snippets plus response digests rather than full API-response bodies. Search
  index churn, literal false positives, deleted/renamed code and SaaS behavior
  remain investigation limitations. A digest without retained response bytes
  cannot reconstruct those bytes.
- `reviewed_license` is historical provenance for this reviewed catalogue, not
  an independently authoritative or general-purpose REUSE resolver. Updating
  origins requires source/license review. A protocol literal can appear in a
  lawful independent implementation without copied protected code.
- Native GitHub Development linkage is not exposed by the publication tools.
  The owning issue must record this limitation explicitly; a capable publisher
  can establish it later without changing reference provenance.

No downstream reuse was investigated or accused, no production identity was
created, and no monitoring was activated as part of this handoff.
