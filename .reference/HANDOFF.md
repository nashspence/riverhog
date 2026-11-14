# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [Riverhog #964](https://github.com/nashspence/riverhog/issues/964), specifically
[the shared-primary-member request](https://github.com/nashspence/riverhog/issues/964#issuecomment-6870475991)
and its [focused lifecycle follow-up](https://github.com/nashspence/riverhog/issues/964#issuecomment-6871280583).
Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Audited base: `codex/964-session-owner` / PR #979 at
`f35123c4dcb79de171627257c72633b9d72378f0`.
This extends the published reference `f507c2eb585d0af9d3aa142ce7a88ab75f7fd980`
without rewriting it. The only authoritative-source changes since its original
audited base are `mise.lock` and the terminal-due/dependency-policy tests.
The reference history records that source update explicitly. The integration
follow-up says no proposed product files from the initial slice were adopted.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro. Mode: Default, with parallel
source, lifetime and evidence review. No undisclosed backend configuration is claimed.
Requested by: repository maintainer `nashspence`, responding to the integration
agent's focused request. Published by: OpenAI ChatGPT through the maintainer's
authorized repository connection. Commit account metadata is publication metadata.

Purpose: replace repeated primary-member source validation with current admission
of accepted Native facts, bounded preparation, historical indexed selection and
shared producer checks. The focused extension adds durable application ownership
and bounded restart reclamation of the two cross-turn derived indexes. It does
not supersede the adopted selected-prefix or imported-primary references.

**First step:** read [the registered lifecycle](DESIGN.md#registered-lifecycle-and-crash-recovery),
then run `tests/unit/test_primary_facts_lifecycle.py`. The existing real-caller
shared-fanout witness remains in `tests/unit/test_shared_primary_admission.py`.
Use [the reproducible verification entry](VERIFY.md) for exact commands and evidence collection.
Reconcile the small caller diff and owner lifecycle with the then-current PR source
before deciding whether to adopt or independently reproduce it.

Product paths contain proposed reference edits. Generic provider fences and the
complete current raw terminal barrier retain their existing implementation.
New tests exercise real `ArchiveCatalogFinalizer.advance` calls, including original
receiving; fixture injection supplies small exact sources, not a production load.

Validation and the exact tested/published source identities are recorded in the
owning-issue publication comment. Source-owned declaration samples are verified
by the existing `scripts.validation_witnesses.collect_test_witnesses` collector.
The branch includes focused semantic, primitive growth and receiving tests;
their scope is described in [VERIFY.md](VERIFY.md).

Scope of the extension: one new application catalog table and its explicit
pre-v1 SQLite/PostgreSQL baselines; one registered namespace holding both exact
SQLite file sets; committed allocation and custody before database effects;
exclusive bounded retirement through the existing shared registry; and the
small Native/neutral placement and identity-check interfaces those owners need.

Known limits: derived facts rebuild after restart; durable lifecycle records
are disposal authority, not reusable source proofs. Full PR CI, PostgreSQL
execution, built/backend qualification, the synchronous outer terminal
transaction, whole-path G13 and large-envelope acceptance are not established here. See
[remaining integration boundaries](DESIGN.md#remaining-integration-boundaries).

This branch is concrete externally produced input, not an integration branch.
Its creation, request, validation or publication establishes no accepted design,
protected contract change, release requirement or authority to merge. The owning
issue, subsequent maintainer decisions and normal integration rail control.
Do not merge or mechanically apply this reference. The exact published commit
linked from #964 is the durable handoff identity; the branch name is navigation.
No native Development association or closing PR is requested or expected.
