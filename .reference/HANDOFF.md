# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#1005](https://github.com/nashspence/riverhog/issues/1005). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).
Requested response: [integration agent comment on #994](https://github.com/nashspence/riverhog/issues/994#issuecomment-7100477049).

Audited authoritative base: `main@be483fd6d567616d5fcd355202d61f1e682efe75`.
Implementation and experiment source: `codex/994-durability@3d039b382ee33d66c6049dd69e498169aa75bf6d`.
The issue's reproduction source is `fee80a186ae31765addfbd4b2e3f877f9eb8b286`; the physical execution owner is byte-identical at all three commits.

Prepublication reconciliation: the integration branch advanced to `0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f` with two finalization-test changes outside the proposal's paths. The physical execution owner is still unchanged. Validation remains pinned to the experiment source above; no qualification of the newer tip is claimed.

Produced by: OpenAI ChatGPT.
Model: GPT-6 Astra Pro.
Configuration: collaborative code execution and independent source/test review; no more specific reasoning setting is attested.
Requested by: Riverhog integration agent; publication requested by maintainer `nashspence` in ChatGPT.
Published by: OpenAI ChatGPT through the connected GitHub account on the maintainer's behalf.

## Purpose and first step

Provide a focused, executable proposal for interruption-safe **original native worker exit receiving**. The unchanged owner reproduces false completion after real SIGINT; the proposal retains the original slot, resource lease and capacity until a positive native exit receipt. This does not implement durable Core settlement or resume independent #991 work.

**First verification step:** follow [VALIDATION.md](VALIDATION.md) to apply [implementation.patch](implementation.patch) in a separate checkout of the exact experiment source and run [verify.py](verify.py). It compares the same maintained real-interruption probe against unchanged and proposed physical execution owners before running the focused suite.

[DESIGN.md](DESIGN.md) explains the native primitive, source research, runtime requirements and remaining integration decisions. [evidence/results.json](evidence/results.json) records the actual final validation; [proposal-manifest.json](proposal-manifest.json) pins the proposed files and patch.

## Source relationship and authority

This is a new handoff, with no earlier #1005 reference superseded. The reference branch has the authoritative base as its single parent; **every product path in this branch remains identical to that base**. Proposed implementation and tests exist only in `.reference/implementation.patch`, targeted and tested against the separately identified integration checkpoint. Testing that patch is not a claim that current `main`, all of #994, or an integration merge is qualified.

This branch is externally produced reference material. Its creation, testing, publication, issue reference, or request by an authorized repository agent does not itself establish or extend an accepted design, contract, release requirement, or authorization to integrate these changes. Authority remains with #1005, subsequent maintainer decisions and the normal integration rail. Reconcile against then-current source; do not apply mechanically. The exact published reference SHA recorded in the owning issue is the handoff identity; the branch name is navigation only.

## Validation and limits

Performed: **342 passed / 1 Darwin-only skip** in the focused verifier; **363 passed / 1 skip** in the broader overlapping suite including generated-contract boundary checks. All eight actual unchanged-owner SIGINT cases reproduced the defect. The proposal passed original outcome, custody/capacity, Python and native TLS, effect/reply-loss, measured native/Core growth, normal/editable wheel, static and qualification-selection checks. Exact counts and commands are in [VALIDATION.md](VALIDATION.md).

Not performed: Windows or macOS execution; whole-repository/release/provider qualification; host power-loss testing; durable Core physical-bit or cost-settlement integration. Windows is unqualified proposed code. Darwin worker construction explicitly refuses because this packet supplies no proved bounded backend. The Python bootstrap adapter is deliberately restricted to GIL-enabled, main-interpreter CPython 3.12, with the pinned 3.12.3 profile executed here. This is a useful Linux reference, **not a cross-platform merge-ready implementation**.
