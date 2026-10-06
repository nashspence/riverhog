# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#958](https://github.com/nashspence/riverhog/issues/958)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Reference branch: `reference/external/958-archive-durability`
Audited base: `main` history at `f5d09bfdaef74d8729362df449ecbe0441b9c19c`
Reconciliation checkpoint: `main` at `f4ac676554d5e68cae629f79af618ce35e278a43`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro (user-visible model identity)
Configuration: No separate effort/mode setting exposed.
Requested by: Riverhog maintainer, through the connected ChatGPT conversation.
Published by: OpenAI ChatGPT through the connected GitHub publication tools.

## Purpose and contents

Provide an independently reviewable implementation input for #958, not another
integration branch. `patches/01-payload-read-domains.patch` contains the four
source edits and six new unit tests. `patches/02-v1-readability-policy.patch`
is a separately reviewable compatibility-prose proposal. Neither patch is
silently applied to the product source on this branch.

The fixed captures and replay tests under `fixtures/` and `tests/` exercise
historical archive semantics, embedded source-root preimages, copy-adjacent
metadata, and supplied filesystem/S3 backing representations. They are a
pre-v1 fixture-method prototype, not a newly accepted historical-support policy.

`INTEGRATION.md` gives application order, scope boundaries, and precise remaining
work. `verify.py` applies only patch 01 in a fresh detached worktree and runs the
focused checks. `validation/` records the executed check and artifact hashes.

## Authority

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, native issue linkage, or request by an
authorized repository agent does not establish or extend an accepted design,
contract, release requirement, or authorization to integrate these changes.
Authority remains with #958, subsequent maintainer decisions, and Riverhog's
normal integration rail. Reconcile against then-current source and issue
comments; do not merge this reference branch or apply it mechanically.

The exact published reference commit recorded in #958 is the durable handoff
identity. The branch name is navigation. The publisher may reconstruct this
reference tree using GitHub's Git data API; in that case the actual GitHub
commit, not a local staging commit, is the published identity. Publication must
preserve the audited parent and complete reference tree exactly.

## Base choice and current-source reconciliation

The source capture and implementation experiment use the original audited,
complete f5d09bf checkout. The available clone gateway was at capacity and this
environment could not fetch Git directly. GitHub comparison established that
the newer f4ac676 checkpoint is two commits ahead: qualification and onboarding
changed, but none of the four patched source files, `release.toml`, or the
captured archive/adapter implementations changed. The original audited base
is retained for reproducibility, not presented as the current main tip.
Use the newer integration rail, including `make linux-qualification`, when
validating accepted integration.

## Validation actually performed

- Reproduced and corrected the descriptor and pack-index acceptance mismatches.
- The real 50,000-empty-member rendered-pack regression also exposed rejection
  of adjacent zero-byte members after the index-budget obstruction was removed.
  Patch 01 fixes the extent comparison and retains overlap rejection.
- Final focused run: **217 passed, 18 skipped**; exact command/log and environment
  limitations are in `validation/`. The skipped tests require external `age`
  and `age-plugin-batchpass` executables.
- The 256-part test uses real 16 MiB encrypted data and substitutes Python age
  decryption only at the missing external-executable boundary. It is a payload
  recovery-closure test, not complete-archive or independent-crypto proof.
- The frozen semantic capture was produced from the pristine audited worktree,
  not from a candidate writer during replay. Both service and independent
  recovery provenance readers consume its fixed bytes.
- Actual filesystem small-object and completed segmented-object state was
  captured from the pristine base. Tests materialize it without starting the
  writable adapter, check source-file fixity, require the segment ledger, and
  separately exercise normal adapter reads. The S3 provider envelope is
  explicitly synthetic; its mapping/metadata encoding was captured from base.

## Not validated / limitations

No full locked installation, Ruff/mypy, complete `make lint`, `make unit`,
`make dist-smoke`, `make build`, `make linux-qualification`, provider qualification,
release/atlas generation, or complete external-age recovery proof was run.
The environment used Python 3.13.5 and installed dependencies via source imports,
not the repository's complete lock. An environment-only copy of the rfc8785
0.1.4 algorithm, with annotations/docstrings reduced, filled a missing dependency;
it is not committed or offered as a supported substitute. Repeat all checks in
the locked environment before integration.

The decoded semantic fixture keeps `.age` logical path labels but contains
**plaintext** bytes. It cannot be passed to the recovery CLI as a stored archive.
The fixture supplies structural/semantic oracles, not ciphertext interoperability.
No historical published v1 release exists in this experiment; pre-v1 captures
may be replaced after deliberate hard cuts. Preserve the final accepted baseline
thereafter rather than continually regenerating the oracle.

This handoff does not finish #958's state-owner registration, contract-atlas
coverage, full service/installed recovery qualification, or post-major support
decision. No old/pre-v1 reader, alias, live-store migration, cleanup, main/release
write, pull request, issue closure, or provider workflow is authorized by it.
GitHub check status and native Development-link availability are recorded in the
owning issue after publication; local success is not a CI-success claim.
