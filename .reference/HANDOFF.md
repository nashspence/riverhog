# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#987](https://github.com/nashspence/riverhog/issues/987)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/987-retained-posix-receiving`
Audited base: `codex/964-session-owner@0e45d35a64d18dbde0f88b89d3c55d60b4977d28`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Additional configuration/effort identity: not attested
Requested by: Riverhog maintainer, for the integration agent
Published through: connected GitHub integration

## Start here

Read [CLOSE_DESIGN.md](CLOSE_DESIGN.md). Choose an already-retained, single-lifetime
native resource object for both regular files and directories. It commits each
native acquisition/close outcome before returning to Python. A close error is
quarantined, not retried by integer. A lost Python reply reuses the exact original
object and its recorded result. Parent custody survives every unresolved child.

**First integration slice:** replace the neutral extent helper's frame-local
file/directory handles with a caller-retained turn. Re-run #987's metadata-phase
close-refusal witness on the actual DiscoveryRetirementTurn. It must retain the
original data owner and parent lease after advance raises, then close the exact
owner once. Reuse the released parent descriptor for an unrelated file and for a
new open of the same inode; retry must not close either. See [INTEGRATION.md](INTEGRATION.md).

This is a concrete reference implementation, not a callbacks-only proposal:
[the native owner](retained-posix/native/owner.c), [the neutral retained extent
borrower](retained-posix/src/retained_posix/extent.py), and [actual descriptor
witnesses](retained-posix/tests/test_receiving.py). The package imports no Core,
Native, archive, database or scheduler implementation. No ownership architecture
change is requested; the native syscall/result boundary is explicit.

## Evidence and source relationship

Only `.reference/` files are added; all product paths/modes match the audited
public base. The issue's unpublished local d51db81d source was not inspected.
This is the initial #987 handoff. It neither supersedes the #984 provider protocol
nor rewrites #964's retirement/authority decisions.

The installed fault-witness wheel executes real Linux openat/fstat/fcntl/close
operations. A separate normal installed wheel exposes no fault controls.
[RESULT.json](RESULT.json), [GROWTH_RESULT.json](GROWTH_RESULT.json) and
[VALIDATION.md](VALIDATION.md) distinguish actual results from remaining qualification.
The source-owned growth declarations use the maintained growth validator; native
syscall counters are measured, not inferred from successful method return alone.

The runtime/compiler here are not the repository-locked runtime. Darwin is a
selected compile profile but was not executed. No maintained Core/Native turn,
PostgreSQL, product distribution gate or complete retirement qualification is
claimed. [QUALIFICATION.md](QUALIFICATION.md) gives the finite remaining vectors.

## Authority

This is externally produced, non-authoritative reference material. Authority
remains with #987, maintainer decisions and the normal integration rail. Reconcile
current source before explicit adoption; do not mechanically merge this branch.
The exact published SHA recorded in #987 is the durable handoff identity; the
branch is navigation. No native Development association is requested or expected.
Record finite consumption and remaining qualification in #987. Publication does
not activate maintenance, complete #987/#964, or authorize release/provider work.
