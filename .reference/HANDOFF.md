# NON-AUTHORITATIVE EXTERNAL REFERENCE

- Owning issue: [#995](https://github.com/nashspence/riverhog/issues/995)
- Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
- Controlling design: [#994](https://github.com/nashspence/riverhog/issues/994)

Audited base: `codex/964-session-owner` (#979) at **0f8030b0c0cd53eb33456924f070ae5979ed307e**. #995/#994 explicitly establish the current integration source; this is not a reset instruction. Main was observed at `209481c189f7387557a85e1c249184642816e103`.

- Produced by: OpenAI ChatGPT
- Model: GPT-6 Astra Pro
- Configuration: Default mode with delegated policy/source reviews; reasoning/effort setting not exposed.
- Requested by: nashspence, Riverhog maintainer, in the current conversation.
- Published by: OpenAI ChatGPT through the connected GitHub tools; GitHub commit metadata identifies the publisher account, not independent producer provenance.

Purpose: give the integration agent a focused, executable policy proposal and specific accounting/configuration/Native-Core contracts for #995 without reopening #994's selected design.

**Current reading entrypoint: [IMPLEMENTATION.md](IMPLEMENTATION.md).**

**First integration step:** on the then-current #979 source, wire the common policy and surviving `ArchiveWriteControlRecord` root into `UploadJournalValidationOwner`; replace per-advance `jobs.publish_progress()` with live processed progress and generation/result-only durable adoption, together with the #996/#997 F1 path. Reconcile current source and issue decisions first.

Contents: the pure policy implementation, an accounting protocol simulation and their deterministic tests under `implementation/`; the concrete integration proposal; [qualification instructions](QUALIFICATION.md); and [actual validation](VALIDATION.md). The classification inventory and handoff identity are recorded on #995.

Source relationship: **all product paths match the audited base exactly**. Only `.reference/` is added. No production consumer/configuration/backend is represented as implemented. The accounting simulation is not a persistence or authorization implementation. This is the first handoff on this branch; no earlier reference SHA is superseded.

Validation performed: 52 deterministic tests using pinned Python 3.12.3; pinned Ruff 0.16.0 lint/format checks; strict mypy 2.3.0 checks on the two implementation/model modules; source/policy review; unchanged-product-tree verification. Exact commands and scope are in [VALIDATION.md](VALIDATION.md); the owning issue records verification of the published tree at its exact SHA.

Not validated / known limitations: repository `make lint` could not start because `mise` is unavailable. Full locked workspace, installed/backend/platform/runtime/growth/CI qualification and real Native checkpoint/profile/restart measurements were not performed. The model provides no measured performance, actual checkpoint retention, crash-survival or #995 completion claim. Finite measured owner profiles and the real vertical slice remain necessary integration work; the proposal specifies how to do them.

This branch is externally produced reference input. Creating, testing, publishing, linking or requesting it does not establish or extend an accepted design, external contract, release requirement or authorization to integrate. Authority remains with #995/#994, subsequent maintainer decisions and Riverhog's normal integration rail. Reconcile and explicitly reuse/reproduce accepted material; do not apply this branch mechanically or copy the reference work documents into main.

Reference navigation: `reference/external/995-replay-policy`. The **exact published commit SHA recorded on #995** is the durable handoff identity. No native GitHub Development association, closing PR, issue closure, merge, release or provider operation is part of this handoff. After consumption, record a finite adopted/reproduced/partial/superseded/rejected disposition and remaining qualification on #995.
