# Reference validation: snapshot review, 2026-10-02

This is reference-design/model validation, not production Stove0, NVENC or release
qualification. No production source, state baseline or generated contract is changed.

## Exact downloaded snapshots

Fetched with GitHub (Additional Tools), then verified locally:

| Identity | Latest main | Previous reference |
| --- | --- | --- |
| Commit | `88a3b3ae7d0addec10ced70b9eb04c7f96e0332f` | `6d5e3985fb9c781052594641773ba36ea1e05b30` |
| Git tree | `cc681ea890d956ab2fccb45fa0da61e33d12d29d` | `e30acea7a90f540477d5bf060c595500b87c17ea` |
| Files | 6,558 | 6,555 |
| Archive bytes | 22,249,948 | 22,244,982 |
| Archive SHA-256 | `b50271378539ce0e3f7b4ecbd15535eb1e79198f53edbef997ec5e654322c994` | `b5ba6cb5b3fad880e9d62bdb58d9d182753a82baf3878cbce7b9fc08ebf5adc9` |
| Manifest SHA-256 | `021641c1f0864ced695f9022b2d1549204a2fd091e183fc43c4118b62123319f` | `63757c51fbb5afc4bd064b25bb7012f1b995bb5096b02d1f9d578b36ca34a792` |

Checked ZIP integrity, safe extraction paths, all manifest file sizes/SHA-256/Git
blob hashes and reconstructed Git trees. Removing the six `.reference/` files from
the prior reference reconstructs original base tree
`3847c6b32acf67c8e96518b30a5bfcee7b663911` exactly. All 328 Stove0 files match latest
main in content and mode. Whole-tree comparison found 12 modified non-generated
files, one new non-generated file, 5,224 modified generated files and eight new
generated files on main; only the six reference files are reference-only.

Hash verification covers whole snapshots, not whole-repository semantic review.
The inspected sources/callers/tests are named in INTEGRATION.md. Read current #903,
#948 and its existing handoff comment, plus repository boundary/integration policy.

## Model checks actually performed

Environment: CPython 3.13.5 on Linux, Python standard library only.

```sh
python -m unittest discover -s .reference/948 -p 'test_*.py' -v
python -m py_compile .reference/948/lifecycle.py .reference/948/test_lifecycle.py
```

The unmodified published 28-test suite passed. Eight further tests were then added.
Against the old model, three exposed actual failures: canceled work could resume
after restart, stopped interrupted work could not be canceled, and a foreign-owner
permit could authorize launch. A fourth test required a missing distinction for
independently proven completion after lease loss; it initially failed on the absent
fixture input. The other four additional tests passed already.

After correction, **36 tests passed**, followed by ten further successful runs
(360 additional test executions). Both Python files compiled and parsed under
Python 3.12 grammar. Reference UTF-8/newline/trailing-whitespace checks passed.
Parsing as 3.12 is not running under 3.12.

Tests include 500 denied-admission probes, unrelated-work progress, finite local
capacity, probe expiry, restart, cancellation, stale/foreign grants, exact terminal
replay, authority expiry and uncertain effects. They use SQLite reopening and
synthetic inputs, not actual scheduling latency, network requests or concurrency.
`workers_stopped`, `no_effect` and `completion_proven` are test assertions, not real
proof or proposed security-sensitive API flags. Probe expiry does not terminate a
real thread. Reopening SQLite is not power-loss qualification.

## Provenance verification

Compared the prior published files with the supplied reference-package ZIP. Design,
integration map, lifecycle model and tests matched byte-for-byte. HANDOFF.md and
VALIDATION.md contained publication edits. The publication also changed the supplied
producer identity; HANDOFF.md now restores the package's GPT-6 Astra Pro user-visible
identity and records the correction, without speculating about hidden models.
The prior blanket six-file equality claim is not repeated as fact.

## Not performed

No production implementation is supplied. `make lint`, `make unit`, `make dist-smoke`,
`make build`, generated-contract/state-baseline verification, real HTTP/NVENC/broker
execution, PostgreSQL or multi-process CAS, capability/callback integration,
preview/work-creation continuation, process containment, power loss and fleet load
qualification were not run. Full source snapshots are available now, but the
repository's locked production toolchain/dependencies were not installed for this
reference-only review. Existing production tests were inspected, not executed.

## Publication verification

The amended handoff is published as a descendant of the prior reference, not a
rewrite/rebase and not a merge of newer main changes. The original base remains
`239985271c9ec24942073f4005dd18da621d0251`; latest-main reconciliation is separate.
The actual resulting reference SHA, verified tree and checks are recorded in #948
once publication succeeds. Native Development linkage is not established by this
publisher; the Connector's missing operation is recorded per #903. No issue-closing
keyword or pull request substitutes for that relationship.

The checked CI workflow is push-triggered for `main`/`release/v1` and pull
requests, not this reference branch. Check the actual published SHA via
all-event Actions/check APIs; the Connector's PR-only workflow helper cannot prove
that no other runs exist. No triggered checks means unqualified, not green.
