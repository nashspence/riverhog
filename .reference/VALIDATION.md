# Validation record and limits

## Exact source

Audited product and differential oracle: `efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055`.
Base tree: `3ef257405e9cce1053fc4dd0ad4d44be0f25222b`.
The integration ref was read as that SHA. The requested comment and subsequent
qualified-source comment were both read; neither implements this follow-up.
The existing P15/G10/G11 reference was inspected at
`e35bddf8f1bb72ca3f0046b597d5966cd5a08312` for scope, not used as an experiment base.

The clone gateway archive SHA-256 was
`8b4af78dff8706734e32ca6d543ba69517e5788e40c2720305dbd8d0ed176d51`.
Extracted ZIP executable bits were restored from the Git index; product content
was not edited. The reference publication has one parent, the exact audited base,
and only the eight files in this directory as its delta.

## Checks actually performed

The reference command was:

```
RIVERHOG_REFERENCE_ASCII_ONLY=1 python .reference/probe.py
```

The final run passed 12 tests with zero failures/errors. Exact elapsed time and
15 raw-volume/quantum samples are in `PROBE_RESULT.json`. Differential checks used
the maintained planner, raw/pack plan constructors and serializers at the pinned
source. The proposed frontier was serialized/reopened between quanta. Coverage
includes 1/16/17/129/1,000 raw volumes with quanta 1/3/16, 80 deterministic mixed
pack/raw/empty-member cases, prior pending pack, exact close and byte admission.
At 1,000 raw volumes, quantum 16 needed 63 calls with at most 16 plans per call;
quantum 3 needed 334 calls with at most 3 plans per call. The old oracle deliberately
materializes its result for comparison; this is not a peak-process-memory proof.

The isolated SQLite model exercised hidden staging, separate verification,
rollback of row+cursor and marker+ack transactions, connection close/reopen,
exact retry/lost reply, one-pending conflict, epoch rejection, plan/coverage
corruption, original interval paging across a later append, and bounded abort
cleanup preserving previously published plans. These are model assertions, NOT
execution of Riverhog registration/authorization/custody or process-kill evidence.

Two preliminary runs failed the byte-admission test because its fixture assumed
a numeric grant or the first plan's length would cover all later single plans.
Offsets change serialized length. The test now derives the largest single-plan
length from the independent original oracle, then requires exactly one emitted
plan. No product constraint, implementation integrity check or failure case was
removed. The final passing run includes explicit under-grant refusal.

Static validation resolved 32 named owner symbols across 12 source modules,
parsed both reference Python files, checked two maintained test locations, all
eight reference files and seven relative Markdown links. The first source-anchor
check caught the mislabeled `page_artifacts` name; it was corrected to the actual
`list_artifacts`. The final source/artifact check and staged whitespace check
passed. Product paths and modes match the audited base.

`make lint` was attempted and exited 2 before lint ran: the Makefile prerequisite
reported missing `mise` (recipe exit 127). No lint pass is claimed.

## Dependency/runtime limitation

Scratch runtime: Python 3.13.5, SQLite 3.46.1, Pydantic 2.13.5 and cryptography
46.0.4. The repository instead pins Python 3.12.3 with its locked dependencies.
The missing rfc8785 0.1.4 dependency could not be fetched: uv/PyPI DNS failed;
the separate download route did not obtain a wheel. No substitute serialization
library was installed.

The explicitly selected ASCII-only probe mode inserts a module sentinel for that
missing import. Its dump/dumps functions THROW on any use. Tested plans have ASCII
keys and safe integral values, so the MAINTAINED canonical serializer uses its
own native fast path; the unavailable slow path never successfully executes.
This permits a limited exact-source differential experiment, not general JCS,
Unicode/float, installed-distribution or locked-runtime qualification. Default
probe execution in a complete repository environment uses the real dependency.

## Not validated

No maintained service/HTTP handshake, current authorization, SQLite production
session configuration, PostgreSQL, multi-process race, fsync/power loss, payload,
encryption, Native custody, final archive roots, full cleanup/retention flow,
large-member workload or end-to-end growth was run here. The model assumes a
bounded normalized all-new batch and one construction; the design additionally
requires exact construction scope, mixed existing/new retries and durable client
batch records. The model's fencing/abort calls represent decisions already made
by an authorized owner, not new authorization mechanisms.

Source/static/publication checks and exact reference check status are recorded
in the owning issue at publication. No absent, pending, skipped or ancestor check
is a passing check for this reference. It is proposed design input, not a green
implementation handoff or authority to merge/release.
