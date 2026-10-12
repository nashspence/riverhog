# #968 — test purpose and product-contract evidence

NON-AUTHORITATIVE EXTERNAL REFERENCE under [#903](https://github.com/nashspence/riverhog/issues/903).
Owning scope, decisions and acceptance: [#968](https://github.com/nashspence/riverhog/issues/968).

| Provenance | Value |
| --- | --- |
| Audited authoritative ref | `main` |
| Audited base / direct parent | `152d095038df0c0d0ed642cf265d47b11c65a754` |
| Audited source tree | `991e994457b487a7dca016b7fc0409485cef25c6` |
| Reference branch | `reference/external/968-test-evidence-roles` |
| Producer | OpenAI ChatGPT |
| Model | GPT-6 Astra Pro |
| Configuration / effort | Not exposed; no hidden configuration asserted |
| Requester | Riverhog maintainer, through this conversation |
| Publisher | OpenAI ChatGPT through the connected GitHub publication tools |
| Handoff identity | Exact published reference commit recorded in #968 |

## Start here

Read this file, then **P00 in [INTEGRATION.md](INTEGRATION.md)**. It identifies
one existing real witness path and two non-product controls. Integration can
start there without loading every related issue or inventing a new test engine.
The full issue, not the earlier chat or this illustrative model, owns scope.

This package contains a small executable reference, not a runtime patch or a
replacement implementation. All additions are under `.reference/`; no product,
CI, contract, guide, dependency or release source file is changed. Reuse useful
ideas explicitly through the current integration rail; do not merge this branch
or import its synthetic Python module into production.

## Contents

- [INTEGRATION.md](INTEGRATION.md): first vertical slice, exact source seams,
  consumer tests, expansion order and hand-back format.
- [claim_projection.py](claim_projection.py) and
  [test_claim_projection.py](test_claim_projection.py): 22 executable synthetic
  decision-boundary tests. Fictional claims are fixtures, not Riverhog contracts.
- [source-seams.json](source-seams.json): byte fingerprints of the narrowly
  inspected source files, for reconciliation only; not a test/contract inventory.
- [verify_reference.py](verify_reference.py): local integrity and source-ref
  reconciliation without importing or executing repository code.
- [VALIDATION.md](VALIDATION.md): actual checks and limits.
- [manifest.json](manifest.json): SHA-256 of every other reference file.

## Reproduce the supplied checks

From this reference worktree, using Python 3.10 or newer and Git:

```sh
PYTHONDONTWRITEBYTECODE=1 python .reference/test_claim_projection.py
PYTHONDONTWRITEBYTECODE=1 python .reference/verify_reference.py
```

To compare the focused source seams with another committed integration ref:

```sh
python .reference/verify_reference.py --repo /path/to/authoritative/worktree --ref HEAD
```

Exit 2 means reviewed source seams changed and need reconciliation, **not** a
Riverhog runtime failure. The tool reads committed Git objects; it does not
validate uncommitted edits. Matching seams do not establish integration readiness
or product conformance. Re-read current issue decisions independently.

## Current adjacent authority

[#963 decision 6125400031](https://github.com/nashspence/riverhog/issues/963#issuecomment-6125400031)
controls its corrected evidence/performance distinction. Its current corrected
reference is `141ce510792f9994cbd75aa58e268fb292aa704e`, but that reference is
not integrated main or authority to merge it. Older timing-approval conditions
in historical #963/#964 material must not re-enter through this work. Consult
#968's coordination section for the specific adjacent owners.

## Validation and limits

Performed: inspected pinned source and current owning/convention/adjacent issue
records; checked the clone ZIP and embedded manifest checksums; ran all 22 new
synthetic tests; checked source-seam fingerprints, reference integrity, Python
syntax, relative links and whitespace. Publication tree/parent verification is
recorded in #968 after the actual GitHub commit exists.

Not performed: repository pytest collection or product tests; locked-toolchain
lint/type checks; full local Linux, images, installed artifacts, native/provider
qualification; Contract Closure regeneration, browser reading, or CI qualification.
No reference result closes a product proof gap. Full details are in VALIDATION.

## Authority and publication boundary

Creation, testing, publication, request or native linkage of this branch does
not establish, extend or override an accepted design, product contract, release
requirement or authorization to integrate it. Current #968 decisions and the
normal repository integration rail control. The earlier conversation is intent
calibration only. Producer and requester remain distinct.

GitHub may reconstruct this exact prepared tree as a new commit; the actual
published SHA recorded in #968 is the durable handoff identity. There is no
producer-supplied pre-publication commit SHA to substitute for it. Native
Development linkage, if unavailable to this publisher, is explicitly recorded
in the owning issue as #903 requires. A Markdown link is not that relationship.
