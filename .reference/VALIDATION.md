# Validation scope

This reference publication was checked as a handoff and proposed patch. It is not Riverhog production qualification.

## Checks prepared for this publication

- Proffer's own three contract tests: `python -m unittest discover -s tests` in the source snapshot of `nashspence/proffer@2ce5e15c3191fcdc2be70213c0170f57c852a883`.
- This handoff record: `python research/proffer/check.py handoff/.reference/proffer.json`.
- Proposed patch: `git apply --check .reference/proposed/agents-policy.patch` against exact fetched `AGENTS.md` and `tests/unit/test_entrypoints.py` from Riverhog main `be483fd6d567616d5fcd355202d61f1e682efe75`, followed by application in a temporary verification tree, exact comparison to the proposed files, Python syntax parsing and guide checksum/word-limit checks.
- Reference inventory: UTF-8 file inventory, relative local links, artifact membership and absence of unresolved issue-number placeholders.
- Publication: after GitHub tree/commit/branch creation, compare every reference blob to the supplied archive inventory and verify that every product root entry and sole parent match the audited base. Validate the publication record with the same Proffer checker and post it to the owning issue.

Actual results and exact publication identity are recorded in the owning issue's publication comment. The publication and consumer disposition are intentionally not embedded in the commit they identify.

## Environment and limits

Local reference validation uses Linux, Python 3.12.14 and jsonschema 4.26.0 (within Proffer's declared range). This is not the approved prepared-v2 timing environment, which requires Python 3.12.16 and its GitHub resource contract.

The guide proposal is 2,053 words, below the unchanged 2,100-word cap, and updates the existing explicit guide hash and conflicting policy assertions. Passing patch/syntax/schema checks does not establish the complete Riverhog test suite, shared resource admission, production-operation behavior or required-check enforcement.

Not run by this contribution: Riverhog lint/unit/distribution/image/full CI qualification; PR #1001 runtime tests; prepared-resource setup or timing cohorts; complete publication benchmarks; native/backend/platform fault campaigns; provider or release operations. Six independent production budget values and their approved semantic binding remain unresolved integration inputs. The reference contains no active fake limits JSON.

## Repeat after adapting into product source

Use the repository's pinned tools, focused tests of changed owners, retained failure/interruption/receiving tests and actual final-source required CI. Validate the integrated authority/workflow/adapter through shared verdicts, including correct pass, complete miss, unqualified resource/authority, incomplete timeout, stale candidate and missing evidence. Existing ordinary suite-timing tests must still show that advisory duration misses do not fail validation.

The consumer must report which exact source ran which checks. An inspected newer branch, earlier passing commit, central primitive witness, structural policy job or schema-valid Proffer record is not that evidence.

