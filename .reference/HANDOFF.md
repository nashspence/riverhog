# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#964](https://github.com/nashspence/riverhog/issues/964)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Request: [6551784418](https://github.com/nashspence/riverhog/issues/964#issuecomment-6551784418)
Branch: `reference/external/964-bounded-registration-publication`
Audited base: `codex/964-session-owner@efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: no additional identity attested
Requested by: Riverhog maintainer, for the integration agent
Published through: the connected GitHub integration

## Start here

Read [REGISTRATION_DESIGN.md](REGISTRATION_DESIGN.md). The selected mechanism is
one pending registration per construction, bounded hidden staging and verification,
then one atomic accepted-frontier publication. No payload or custody is admitted
for an unpublished suffix. Earlier published work can continue.

**First integration slice:** with the actual registration service, declare one
artifact spanning 17 raw volumes, admit a quantum of three plans, stop after the
first committed quantum, reopen, and prove zero accepted member/plan/coverage
visibility until one final publication. Then compare every plan byte/volume ID
with the existing planner, and recover the original acknowledgement after a lost
reply. Use [QUALIFICATION.md](QUALIFICATION.md); do not start with a whole P15 rewrite.

The necessary current-only HTTP cut is explicit: pending is not successful
registration. Only published status supplies the atomic acknowledgement. The
unbounded inline volume list becomes an exact immutable range with bounded
keyset pages. SDK execution waits/resumes; status and scheduling do not execute
or wait for the entire plan. This proposed interface/state-machine change needs
explicit consumption under the owning issue; publication does not approve it.

## Material and source relationship

Only `.reference/` files are added. Product paths equal the audited base; there
is no implementation patch. This extends the P15/G10-G11 slice of
`e35bddf8f1bb72ca3f0046b597d5966cd5a08312`, not the entire large-collection design.
No earlier handoff is rewritten, deleted or mechanically merged.

[bounded_emission.py](bounded_emission.py) is a reference planner using maintained
plan types, constructors and serializers. [probe.py](probe.py) compares those
results with the unchanged planner and exercises an isolated SQLite publication
model. [PROBE_RESULT.json](PROBE_RESULT.json) records the actual run.
[RESEARCH.md](RESEARCH.md) supplies source anchors and external primary sources.

Twelve reference tests passed, including 15 raw-volume/quantum samples and 80
mixed planner cases. This is metadata/reference evidence, not service, HTTP,
Native, encryption, custody, PostgreSQL, process-kill or growth qualification.
The scratch environment used a fail-on-use sentinel for unavailable rfc8785;
only the maintained serializer's ASCII/safe-integer fast path was exercised.
`make lint` was blocked before lint execution by missing `mise`.
[VALIDATION.md](VALIDATION.md) records the limitations and actual checks.

## Authority

This is non-authoritative external input. Authority remains with #964, maintainer
decisions, and the normal integration rail. Reconcile the exact integrated source
and current decisions before adopting it. Do not merge the reference mechanically.
The published SHA recorded in #964 is the handoff identity; the branch is navigation.
No native Development association is requested or expected. Record finite
adoption/reproduction/disposition and remaining qualification in #964.
