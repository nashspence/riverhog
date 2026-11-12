# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#988](https://github.com/nashspence/riverhog/issues/988)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/988-native-constructor-receiving`
Audited base: `codex/964-session-owner@c5157d69712185aa8d4ad185e2e6d465dfbd7179`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: no additional identity attested
Requested by: Riverhog maintainer, for the integration agent
Published through: the connected GitHub integration

## Start here

Read [CONSTRUCTION_DESIGN.md](CONSTRUCTION_DESIGN.md), then the first slice in
[INTEGRATION.md](INTEGRATION.md). Retain a passive Native construction frame in
Core before exclusive file creation, namespace sync, connection construction or
validation. The frame keeps original file and independent SQLite receivers even
when no JournalStore or Session is returned. Receiving and worker join stay separate.

The compiled neutral [resource owner](retained-resource/native/owner.c) extends
the #987 idea with Windows dynamic-UCRT regular-file creation/close. Linux/Darwin
retain POSIX file/directory receiving. The Native Windows journal API is NOT made
POSIX-only. Its existing absence of directory fsync is preserved, not replaced by
a fictional Windows CRT directory barrier. Unknown close outcomes remain charged
and non-retryable by integer; successful close replies can be replayed safely.

**First integration slice:** put the original construction frame in Core's slot
before `JournalValidationSession.open_retained`; thread it to JournalStore as a
required argument. Repeat the public initial-creator-close refusal. The slot/lease
must remain until its original creator and independent SQLite scope receive.
Repeat with a genuinely unknown close: no parent release, slot deletion or force
receiving; independent construction must still progress.

## Material and exact source relationship

This is the initial #988 packet. It extends, but does not rewrite or supersede,
#987 reference `c961ff4390b4e4d8581249c47acd999bdc2aa37e` for constructor ownership
and the Windows regular-file profile. No unpublished integration source was used.
Product paths/modes equal the audited base. Proposed implementation and test bridges
are inside `.reference/`; no production patch is silently applied.

One new branch-scoped workflow, `reference-native-constructor.yml`, runs only this
reference branch on native Linux/macOS/Windows. Existing workflows/gates are
unchanged. Its results are reference qualification, not product acceptance. Exact
published-SHA runs and any infrastructure gaps are recorded in the owning issue.

[reproduce_public.py](reproduce_public.py) reproduced the original actual Core leak.
[native_bridge.py](native_bridge.py) and [test_native_bridge.py](test_native_bridge.py)
exercise unchanged Native/Core execution through an explicit test-only replacement
of constructor origins. They are not a production optional ownership mode.
[run_witness.py](run_witness.py) builds independent normal/fault wheels and executes
outside-checkout isolated tests. [RESEARCH.md](RESEARCH.md) pins primary evidence.
[LOCAL_RESULT.json](LOCAL_RESULT.json) records Linux scratch results and growth.
[VALIDATION.md](VALIDATION.md) separates those results from missing product evidence.

## Authority

This is external reference material. Authority remains with #988, maintainer
instructions and the normal integration rail. Neutral extraction is already
requested; reconcile concrete changes and validate the exact integrated source.
Do not merge this branch mechanically, activate discovery maintenance or treat
reference publication as #988/#964 acceptance, merge or release authorization.

The exact published SHA linked in #988 is the handoff identity. No native
Development association is requested or expected. Record a finite adoption,
reproduction, partial-use, superseded or rejected disposition in the owning issue.
