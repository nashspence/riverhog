# Growth-closure revision of the #958 reference

Start with [CONTRACT.md](CONTRACT.md). It distinguishes representation limits,
operation growth, composition, resource lifetimes and evidence. It proposes
specific closure gates without pretending that two new primitive declarations
prove full recovery or every adapter's complexity.

Audited implementation: `main` at `e0036f80410a83cc5eaa9d63c17650ffae72e2bd`.
This is an append-only revision of reference `eb9264132ba150426f804d96b7bd5d1ba7e26365`.
The branch's product tree remains at its original audited parent; **do not merge
it wholesale**. Use a new worktree at the newer audited implementation and apply
only reviewed patches. Earlier handoff provenance and captures remain intact.

## Supplied implementation input

1. Apply unchanged `../patches/01-payload-read-domains.patch`.
2. Apply `03-recovery-growth.patch`. It adds an operation-local, disk-backed,
   verified descriptor index, keeps it alive through full recovery mapping, and
   uses an actually selective `(journal,kind,sequence)` index. It also streams
   age stdout, enforces bounded parent consumption before spooling, and reaps
   failed/abandoned children. Two source-owned declarations use the existing
   growth machinery; 34 new recovery tests include private-source discovery,
   bound/declaration failures, controlled subprocesses and full semantic
   recovery through a Python-age boundary substitution.
3. Apply `04-materialization-cleanup.patch`. It uses monotone rowid traversal
   between bounded cleanup batches, preserving active checkpoint rows. Four
   parameterized tests cover state correctness, query shape and SQLite VM work.
4. Review earlier policy patch 02 separately. It is not applied by this verifier.

Neither new patch changes persisted archive bytes or supplied-adapter committed
backing formats. The disposable recovery index is not archive authority, an
accepted new public API, or a durable checkpoint-format promise. The recovery
package adds a dependency on the existing Apache-licensed growth package; the
lockfile edge is edited consistently but was not resolver-regenerated here.

## Reproduction

With the required dependencies already installed and the audited commit present
in the local Git object database, from a clean reference checkout:

```sh
python .reference/growth/verify.py --worktree /new/empty/path/riverhog-958-growth
```

The verifier checks artifact hashes, prepares a detached worktree at the exact
audited main SHA, applies 01/03/04, copies the unchanged fixed captures, and runs
the selected tests. It never installs dependencies, fetches, pushes, changes an
existing worktree or contacts providers. The old root `verify.py` remains the
historical first-handoff verifier; use this revision's command for growth work.

Primitive probes are independently rerunnable:

```sh
python .reference/growth/probe.py --source /path/to/audited-main --output /tmp/before.json
python .reference/growth/probe.py --source /path/to/patched-worktree --output /tmp/after.json
```

The probe uses synthetic structurally committed journals, actual source readers
and local SQLite checkpoint state; it is not an external-age or live-provider
proof. It also tests large ordinal encodings using a single empty artifact.

## Measured findings

For 64 journals with three segments each (193 nonterminal descriptors), the
scan/header/range sequence falls from **25,414 descriptor opens to 194**; journal
payload opens remain 192. The count is specific to that measured scope, not a
claim that every full-recovery object is read once. Source hashes and smaller
shape-family results are in `validation/before.json` and `validation/after.json`.

With 8,192 active and 8,192 abandoned rows per checkpoint table, cleanup falls
from **1,196,576 to 410,222 SQLite VM instructions**, preserving active state.
The adversarial generation ordering puts active entries before abandoned ones
in the covering-index order. Different orderings can mask the repeated scan.
VM figures are engine-specific regression evidence, not portable CPU budgets.

The ordinal probe accepts `2^53` but rejects `2^53+1` in pack construction even
though both have valid outer 256-bit encodings. This still occurs after these
reader patches. CONTRACT.md recommends an explicit exact-encoding pre-v1 cut;
this reference does not silently make that representation change.

## Validation and remaining gates

The final selected suite: **297 passed, 18 skipped**. A clean-worktree replay is
recorded separately in `validation/replay.log`. The skips require official `age`
and `age-plugin-batchpass`. The controlled-child tests validate subprocess
lifetimes, not age interoperability. The full semantic recovery witness uses
real Riverhog encryption/decryption with a Python boundary substitute.

Python 3.13.5, SQLite 3.46.1 and source imports were used, **not a locked release
installation**. Package installation was unavailable. A test-only transcription
of upstream rfc8785 0.1.4's implementation was used; it was not byte-identical to
the upstream file and is not committed or proposed as a supported dependency.
Canonical-JSON tests pass in that environment, but this remains a qualification
limitation. Exact environment and tested-source hashes accompany the results.

No full lint/type check, full repository unit gate, distribution/image/build,
Linux qualification, atlas/release generation, official external-age recovery,
Windows child-process test, or live-provider qualification is claimed.

Whole-operation CPU/RSS/scratch coefficients, KDF/header/time admission,
malformed tar pre-allocation bounds, selector growth, full history/proof graph
composition, state-owner registrations and the final historical v1 baseline
remain explicit integration gates. Required per-member sidecar output is an
independent dimension; a bound linear only in unique metadata is insufficient.
This reference supplies concrete corrections and a tighter design, **not issue
completion or authorization to integrate**.
