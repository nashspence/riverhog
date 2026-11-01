# Validation record and limitations

## Exact source relationship

Audited product source and reference parent:
`533ac4ee06def4692288c39c9279bf9387525a3e`.
Base tree: `c9db76b1f87b63db98321755ffa41d2044e1f45c`.
This is the integration checkpoint explicitly named in #984. Additional source
`03dfc3c433372e5a965d37bbc5faf336feb05f17` was inspected for drift, not used as a
product execution source. Its delta has 30 files, including cleanup identity
factoring, snapshot handling and fixture corrections. Raw/pack upload and the
reviewed store implementations are unchanged across those two commits.

The current-source full clone archive was verified with SHA-256
`d376c5a7204e98de386918e05eb028412a6b53876e649063e0a0ca2503222da4`.
The issue source was checked out in a separate clean worktree. Product paths/modes
remain unchanged. Only nine `.reference/` files form the intended publication.
The exact published tree, parent, branch tip and check status are recorded in #984.

## Checks actually performed

The reference command was:

```
python .reference/probe.py
```

It passed **22 tests**, zero failures/errors, in **1.744572581 seconds** on
Linux, Python 3.13.5 and SQLite 3.46.1. The exact result is retained in
[PROBE_RESULT.json](PROBE_RESULT.json). This was one successful model run;
no maintained service or provider code was imported or executed.

The model covers the original delete-before-delayed-begin ordering as a negative
control; admission before dispatch; late begin/part/complete; no next effect after
closure; late-seal cancel refusal and discard reconfirmation; empty observation
versus closure; exact checkpoint replay/rollback; stale report conflicts; independent
construction progress; indexed bounded unresolved lookup; concurrent terminal
selection; exact challenge retention through expiry/partial cleanup/reopen;
cleanup rollback and stable terminal outcomes. Events/barriers impose ordering;
test deadlines only detect hangs. One actual subprocess exits via `os._exit(71)`
after committing admission, and cold reopening retains the unresolved call.

`ClosedEffect` and the cleanup-driver confirmation are deliberately trusted MODEL
inputs. The probe does not establish that S3/filesystem/HTTP can produce closure
evidence. It uses a simplified boolean custody fact, not Native/sealed receipts.
The toy schema retains settled rows to test an unrelated backlog; product compaction,
current permission evaluation, orphan challenge authenticity, claim renewal locks,
provider pagination and full resource accounting are design/qualification work.
Connection reopen/process exit is not power-loss durability evidence.

Static checks parsed 15 source/test modules and both reference Python files,
resolved 35 named source symbols, checked eight maintained test locations, Markdown
fences/newlines and all relative links, and checked the recorded JSON result.
The first anchor check exposed a mistaken lookup symbol in the checker itself;
it was corrected to the actual `find_completed_write` before the final check.
The final staged whitespace and reference-only changed-path checks passed.
No source-symbol check imports or validates the production runtime.

`make lint` was attempted in the exact-base worktree but stopped before lint ran:
`mise` is not available. Make exited 2; the prerequisite reported exit 127.
No lint pass, locked test pass or green implementation delivery is claimed.

## Toolchain and research limits

The repository pins Python 3.12.3 and uv 0.11.24. The scratch runtime is different;
installed unlocked product dependencies were not substituted for maintained test
evidence. An attempt to obtain the missing `rfc8785==0.1.4` dependency failed at
PyPI DNS resolution (uv exit 2). No serialization sentinel/substitute was used.
The delivered reference model uses only the Python standard library.

The source audit and primary external documentation are in [RESEARCH.md](RESEARCH.md).
The issue's four maintained in-memory-provider reproductions and their 2.009-second
timing are prior integration evidence, not this reference's experiment. Native
provider durability, mutation retry behavior and cancellation semantics must be
qualified on the locked implementations, not inferred from the reference model.

## Not performed

No actual upload service, PostgreSQL, provider HTTP transport, S3, filesystem
adapter, cache-mirror, encryption, Native/custody/root, installed distribution,
multi-host, full-volume, machine-reset, power-loss or end-to-end growth qualification
ran here. The real maintained-owner witness plan is [QUALIFICATION.md](QUALIFICATION.md).
No finite-time resolution is claimed for an ambiguous remote effect without actual
provider/executor closure evidence. That selected retained-uncertainty policy needs
explicit issue consumption; reference publication does not adopt it.

The reference commit is not a product implementation. Missing/skipped/pending checks
and ancestor CI are not passing checks for this source. No release, provider
qualification, merge, issue closure or native Development association is implied.
