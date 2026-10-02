# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: https://github.com/nashspence/riverhog/issues/949
Convention: https://github.com/nashspence/riverhog/issues/903
Reference branch: `reference/external/949-removable-media-foundations`
Audited base: `main` @ `ec544b60dde572ac3bc2f31b199ab5b3727e4548`
Audited base tree: `f9c514d5d7c08e82a96f100eeeb48f43507f28c3`
Date: 2026-10-02

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro (user-visible identity)
Configuration: no separate effort/mode setting exposed for this handoff
Requested by: repository maintainer, in the current conversation
Published through: the connected GitHub API, on the requester's behalf

## Purpose and contents

Foundational, source-grounded semantic witnesses for future operator-managed
removable storage support. The material isolates caller ownership, nonterminal
waiting, cleanup obligations, incarnation fences, and bounded read progression
before selecting or freezing a wire design.

- `.reference/949/semantic_model.py`: standard-library-only serial state model;
  no adapter, actual data I/O, database, server, or device implementation.
- `.reference/949/test_semantic_model.py`: 36 semantic tests, including 180
  distinct replay/release schedules with restart snapshots.
- `.reference/949/README.md`: run instructions, pinned source-to-witness map,
  primary-source research, interpretation, and integration limitations.

All additions are under `.reference/`. Production code, public schemas, generated
contract closure, migrations, release baselines, workflows, README, and architecture
are unchanged. No pull request, release synchronization, or main-branch change is
part of this handoff.

## Authority

This branch is externally produced reference material for #949. Its creation,
testing, publication, native issue linkage, or request by an authorized repository
agent does not establish or extend an accepted design, contract, release requirement,
or authorization to integrate these changes.

Authority remains with the owning issue, subsequent maintainer decisions, and the
repository's normal integration rail. The exact published reference commit recorded
in #949 is the handoff identity; the branch name is navigation only. Reconcile this
material against then-current authoritative state and issue decisions; do not apply
it mechanically. In particular, model-only types and literals are not a v1 API.

## Validation performed

- Read #903, its comment, #949, repository guidance, and relevant source/contracts
  at the exact audited SHA; confirmed `main` resolved to that SHA during this work.
- Consulted the primary external sources linked in the experiment guide.
- Ran `python3 -m unittest discover -s .reference/949 -p 'test_*.py' -v` with
  Python 3.13.5: 36 tests passed, including the 180-schedule witness.
- Compiled both Python files with `python3 -m py_compile`.
- Checked the supplied files for trailing whitespace and merge-conflict markers.

Publication verification (actual commit, parent, tree/blob identity, and visible
GitHub Actions status) is recorded in the owning issue after publication, not
predicted by this pre-publication document. No local producer commit SHA is claimed.

## Not validated / known limitations

- No Riverhog unit/integration/conformance suite, `make lint`, `make unit`,
  `make dist-smoke`, or `make build` was run. The execution container could not
  resolve github.com for a clone; repository inspection used the connected API.
  The model is intentionally executable without a repository dependency install.
- Atomic effects and durable JSON snapshots are modeling assumptions, not a
  crash-consistency proof. No real database transactions, process-kill recovery,
  concurrent workers, HTTP framing, authentication, encryption, or payload hashing
  are implemented or tested.
- No disks, mounts, flush/unmount/eject operations, AWS account, webhook receiver,
  Home Assistant instance, or real physical deletion were exercised.
- Finite fence retention, abandoned ownership, renewal/expiry, stream reconciliation,
  cache capacity/fairness, and cross-target one-slot copying remain unimplemented.
  The tests explicitly distinguish these from their narrower modeled properties.
- There is no selected transport/error/capability shape, production state migration,
  generated-schema update, or claim that #949 is complete.
- The available GitHub connector exposes branch creation but no native issue-branch
  Development-link mutation. Record that limitation in #949; a capable publisher
  can add the relationship later without changing this reference commit.
