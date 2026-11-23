# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#1005](https://github.com/nashspence/riverhog/issues/1005). Convention: [#903](https://github.com/nashspence/riverhog/issues/903).
Requested revision: [integration agent startup follow-up](https://github.com/nashspence/riverhog/issues/1005#issuecomment-7102561968).

Audited authoritative base: `main@be483fd6d567616d5fcd355202d61f1e682efe75`.
Implementation and experiment source: `codex/994-durability@0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f`.
Superseded reference: `3dce6780c4883ad96cb32ff3f7ef80883fef6528`; its history is preserved as this revision's parent.

Produced by: OpenAI ChatGPT.
Model: GPT-6 Astra Pro.
Configuration: collaborative code execution and independent source/test review; no more specific reasoning setting is attested.
Requested by: Riverhog integration agent; continued publication requested by maintainer `nashspence` in ChatGPT.
Published by: OpenAI ChatGPT through the connected GitHub account on the maintainer's behalf.

## Purpose and material revision

Remove bootstrap waiting and bootstrap registry work from Core coordination while retaining the original native worker and all prior physical-exit protections. The previous reference was partially used for source review and independent reproduction, without adoption. Its Python startup acknowledgement loop blocked independent progress under Core's execution lock.

The revised wrapper constructs only fixed pending identity on the coordinator and returns from start after the native creation call. The original worker owns ordinary Thread initialization, pending weak-registry drains, bootstrap registration and cleanup. A bounded post-exit observation diagnoses missing acknowledgement. Core retains the original unset Future, slot and capacity on bootstrap failure, permits independent receiving, and drains later workers before reporting an already-received bootstrap diagnostic. Collection measures at most one own weak-reference retirement per removed worker.

The native C implementation is unchanged. Existing real SIGINT, target/Python/native TLS, exact outcome, lease/capacity and lost-reply evidence remains in scope. New maintained witnesses reproduce the exact previous reference's delayed-bootstrap failure, cover interrupted creation replies and pre-acknowledgement exit, and measure actual Python wrapper waits and registry work alongside native and Core primitives.

**First verification step:** follow [VALIDATION.md](VALIDATION.md) to apply [implementation.patch](implementation.patch) in a separate checkout of the exact experiment source and run [verify.py](verify.py). The verifier checks source and file identities, reconstructs the exact previous wrapper/Core pair from its pinned reference, and runs both original defect comparisons before the current suite.

[DESIGN.md](DESIGN.md) explains the source research and ownership argument. [evidence/results.json](evidence/results.json) records final execution; [proposal-manifest.json](proposal-manifest.json) pins every proposed file and the patch.

## Source relationship and authority

This revision advances the experiment source from `3d039b382ee33d66c6049dd69e498169aa75bf6d` to `0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f`, whose two finalization-test changes do not intersect the proposed paths. The original physical execution module is identical at both checkpoints, audited main and the issue's `fee80a186ae31765addfbd4b2e3f877f9eb8b286` reproduction source.

The reference tree still has **every product path identical to audited main**. Implementation and tests exist only in the patch, targeted and tested against the separately identified integration checkpoint. The previous reference is preserved in Git history; this is a new handoff identity, not a history rewrite or an integration commit.

This branch is externally produced reference material. Its creation, testing, publication or request by an authorized repository agent does not establish design acceptance, a contract or authorization to integrate. Authority remains with #1005, subsequent maintainer decisions and the normal integration rail. Reconcile against then-current source. The exact published SHA recorded in the owning issue is the handoff identity; the branch name is navigation only.

## Validation and limits

Performed: **383 passed / 1 Darwin-only skip** in the final clean-checkout selected suite, including all workspace-boundary checks; all 19 verifier checks returned their expected result. Both failing-before comparisons and the new startup/registry/lifetime growth evidence are retained. Exact commands and timings are in [VALIDATION.md](VALIDATION.md). The executed profile is Linux x86-64 with pinned GIL-enabled, main-interpreter CPython 3.12.3. Windows remains an unexecuted draft; Darwin still refuses worker construction because no proved bounded backend is supplied. No whole-repository/release/provider or power-loss qualification is claimed.

The startup correction does not enable durable Core physical receiving or final cost settlement, resolve the separate completion-batch delivery boundary, or resume independent #991 work. This remains focused reference input for the paused Core integration and its applicable platform disposition.
