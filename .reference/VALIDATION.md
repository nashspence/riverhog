# Evidence and limitations

## Exact source and publication identity

Audited source: `codex/964-session-owner@8c94eb3e5e349147d46b8f10fc25e161cb7106be`.
Base tree: `4892d8dee155a682f266fd6aa08d914100321d49`. This is the checkpoint named
by #982, not a claim that the integration has merged into main. Only seven files
under `.reference/` are added; product paths remain identical to that base.
No earlier #982 reference is superseded. The published commit/tree/ref, exact
changed paths and check state are verified separately and recorded in #982.

The full clone archive was obtained through the connected read-only clone service.
Its SHA-256 was `7726f0d5833e67ffac6e92f8fe59e96b6d07fba3e0e6e83e0e12587ef3b68f53`.
After extraction, tracked executable permissions were restored from the Git index;
the source worktree was clean before adding this reference. No product patch,
contract regeneration, runtime dependency change or generated fixture is supplied.

## What ran

The reference [transaction probe](transaction_probe.py) ran successfully:
**7 tests; 3.873348 seconds** overall measured elapsed. The raw machine-readable
result is [PROBE_RESULT.json](PROBE_RESULT.json). Command:

```sh
python .reference/transaction_probe.py
```

It exercises uncommitted/committed selection across abrupt subprocess exits,
unique-name conflict without replacement, SQL control-only access, deletion/cursor
rollback and commit-response loss, and once-only logical reservation release.
The control-access case installs a SQLite authorizer that rejects chunk-table
reads; it is a query-access witness, not a physical page-I/O measurement.
Synthetic byte/receipt fixtures are deliberately not Native or authenticated.

The probe imports no Riverhog modules and is not a backend implementation. Its
simplified schema, whole-fixture construction and local cleanup are not proposed
production code. The seven cases do not establish Win32 sharing/ACLs, StateSchema,
bounded production transactions, durable-byte capacity, actual producer behavior,
or any production growth bound. Process death is not machine power failure.

The scratch execution environment was Linux, Python 3.13.5, SQLite 3.46.1. The
exact SQLite source ID and OS string are retained in the JSON result. Riverhog
pins Python 3.12.3 and uv 0.11.24. No unlocked environment was substituted for a
passing product test claim.

`make lint` was attempted on the audited tree plus reference material. It stopped
at the repository's toolchain prerequisite, before any lint tool ran:

```text
Riverhog Makefile targets require mise on PATH, or MISE_BIN=/abs/path/to/mise.
Install mise or pass MISE_BIN explicitly, then rerun make.
make: *** [Makefile:64: lint] Error 127
```

Make exit: 2; measured command elapsed: 0.005832 seconds. This is an environment
block, not a lint pass and not evidence that product code failed lint. `make unit`,
build/distribution smoke, Linux qualification and native Windows jobs were not run.

## Audit and integrity checks

[RESEARCH.md](RESEARCH.md) lists the inspected owning source and primary technical
references. The review includes the implemented manifest seal/receipt exception,
POSIX filesystem interface, shared store/recovery/closing, physical inventory
scratch bypass, CLI/FTP/target placement, StateSchema behavior and native CI.
The proposed database is not presented as a drop-in directory emulation.

The reference checker parses all nine prepared-primary source modules, producer,
processing writer and StateSchema, checks the cited owner symbols, parses the
probe, checks the five Markdown documents' relative links/fences/final newlines,
and reads the recorded seven-test result. `git diff --cached --check` and the
reference-only changed-path assertion are also run before publication. These
checks establish reference integrity only, not runtime backend correctness. The
initial whitespace check found Markdown hard-break spaces; they were removed
and the check was repeated successfully.

The publication mechanism reconstructs this tree through GitHub's Git-data API.
The resulting tree is compared with the local index tree before creating a commit;
the real published GitHub commit, not an unpublished local identity, is the #903
handoff identity. No native Development link or PR is used for publication.

## Not performed, and what acceptance still requires

There is **no executed Windows backend or power-loss qualification** in this
reference. Native implementation, Windows process/ACL/reparse/flush witnesses,
fresh-container abrupt-reset tests, actual HTTP/Native integration, physical
page/journal/heap evidence, installed packaging and exact-source/tested-merge gates
remain the focused implementation work in [ACCEPTANCE.md](ACCEPTANCE.md).

The supported NTFS/storage assumptions require native evidence, especially new
container name persistence and journal recreation. A Linux SQL test cannot prove
those properties. The design uses the locked engine's documented mechanism and
source-reviewed Windows sync path rather than asserting that file-flush syntax
alone proves arbitrary directory durability.

Prior issue-reported CI and Native-memory observations are not rerun here. No
missing, skipped, pending, blocked or ancestor check is counted as passing for
this reference. Workflow/check status on its exact published SHA is reported in
#982; reference publication supplies design input, not completed #982 acceptance.
