# Riverhog non-binding guidance proposal

This is a proposed source layout and reference implementation, not an applied repository
migration or an adoption decision. Nothing in this bundle changes Riverhog's contracts or
makes the proposed seed entries active. Review the seed before putting adopted entries on main.

Inspected base: `nashspence/riverhog`, `main`,
`e0413a288fe3e817db16d5e887b08622a24b3c94`.

## Recommended end state

```text
guidance/heuristics.toml       # The sole editable register.
guidance/README.md             # Generated plain Markdown, not another authority.
scripts/guidance.py            # Standard-library parser, reference checker, renderer.
tests/unit/test_guidance.py    # Integrity checks plus repository-boundary regression.
```

Use “guidance” as the subsystem name and “heuristic” for a record. The subject can be
implementation, operations, presentation, qualification, architecture, or future contract
design. Subject matter does not change the record's non-binding role.

The whole register has one role: currently adopted, non-binding guidance. There is no
contract classification, authority flag, ownership of contract elements, promotion mechanism,
waiver process, or per-record release gate. Neither the register nor its renderer is an input
to contract generation or interpretation.

A heuristic may inform a discussion about a proposed contract change. That discussion and any
subsequent contract decision remain separate: the heuristic cannot enact or justify an
interpretation of a contract by its presence in this register.

Only deliberately adopted entries belong on reviewed main. Proposed entries remain in review;
Git records changes and retirements. Remove an entry when it is no longer adopted. Do not use
test discovery to infer adoption automatically. No second status machine or retirement archive
is needed for this starting point. Remove the seed's proposal comments only after review.

## Exact data structure

The TOML parser enforces these exact fields and rejects unknown fields at every level:

```text
Register {
  schema: literal "riverhog-nonbinding-guidance/v1"
  heuristics: list[Heuristic]
}

Heuristic {
  id: unique lowercase hyphenated identifier
  guidance: nonempty advisory prose
  scope: nonempty applicability/context prose
  rationale: nonempty explanation of the preference
  related_tests: list[RelatedTest]  # May be empty.
}

RelatedTest {
  node: repository-relative .py path + ::test_function
        or ::TestClass::test_method
  note: nonempty account of the relationship and its limits
}
```

Identifiers are stable references, not versioned promises; use Git history for revisions.
The schema version identifies this internal register format, not a public product protocol.
There is intentionally no title field duplicating the ID, taxonomy to curate, coverage score,
source SHA per record, copied runtime constants, hand-maintained API inventory, or general
contract pointer graph.

Test references are static base nodes, not globs or parameter-case IDs. A parameterized
function can be linked at its base function node. The checker verifies declared functions and
class methods by parsing their source; it neither imports nor collects them, so it cannot
certify pytest collection or a passing run. Dynamic/inherited test targets would need a
separately reviewed extension rather than silently being treated as verified.

Tests can independently exercise a contract and also be relevant to a heuristic. The link
here does not establish a guarantee or change the test's independent role. Removing a
heuristic neither removes tests nor waives unrelated CI or contract requirements. No automated
test is required for a design preference: an explicitly empty list is valid.

## The eight proposed seed records

The complete proposed register is `guidance/heuristics.toml`: eight entries, with 35 exact
associations. These numbers describe this delivery, not invariants the checker freezes.

Four entries are advisory reformulations of records already in
`scripts/implementation_policy.py`, preserving their 28 existing test associations:

| Proposed ID | Existing classification | Design preference being recorded |
|---|---|---|
| exact-external-effect-authority | internal_correctness | Prefer explicit intent and destination-specific ownership before an external effect. |
| structured-authority-dependency-liveness | internal_correctness | Prefer successor protection before releasing predecessor protection. |
| external-result-acceptance-and-cleanup-custody | internal_correctness | Prefer durable acceptance and cleanup receipts through ambiguity and restart. |
| bounded-obligation-progression | operational_policy | Prefer restartable bounded work and explicit accounting of progress. |

These are not declarations that underlying correctness requirements have become optional.
The reformulations themselves need review. The inherited associations are identified as such;
their full test bodies have not been newly audited in this exercise.

Four additional entries are inferences from inspected current source/test assertions, not
claims of prior maintainer adoption:

| Proposed ID | Inspected basis | Deliberate limit |
|---|---|---|
| presentation-is-a-projection | Collection-list, collection-show, and archive-store tests in `tests/unit/test_cli_json_output.py`. | Fake-client checks for those views; no universal field, interface, or live-API parity promise. |
| audience-aware-interface-design | The first two matrix/audience tests in `tests/unit/test_operation_qualification.py`. | Consider intended audience during future design; do not require every operation to acquire every interface. |
| derive-references-from-canonical-sources | `AGENTS.md` source-of-truth guidance and `test_installation_artifacts_are_derived_and_mutually_consistent`. | Prefer generation; do not appoint new contract authorities or claim real-platform qualification. |
| scope-evidence-to-what-was-exercised | `test_exact_sha_evidence_contains_only_generated_current_rows` and scoped evidence descriptions in `scripts/extent_witnesses.py`. | Keep claims narrow; a constructed timing record is not proof of broader behavior. |

I did not seed broad platform/provider equivalence, “every endpoint has a CLI,” or a blanket
HTTP/client/human/JSON parity rule. The inspected material does not justify those broad
non-contractual adoption claims. This seed is not an exhaustive inventory.

## Contract boundary and migration

The current registry contains `exact-read-authority-lifetime/v1`, explicitly classified as
`external_contract`. It is deliberately absent from the new register. Do not simply change
that classification to make the row fit. Its guarantees, where actually established, remain
with their independently designated authorities.

Preserve its five existing associations in the appropriate contract-side evidence/accounting
context, without treating its summary prose as a new source of contract meaning:

```text
tests/unit/test_catalog_sync.py::test_catalog_sync_cursor_fails_closed_for_authority_changes
tests/unit/test_catalog_sync.py::test_catalog_sync_cursor_expiry_is_explicit
tests/unit/test_catalog_sync.py::test_catalog_sync_history_reaping_has_an_explicit_gap_error
tests/unit/test_runtime_config.py::test_catalog_sync_cursor_lifetimes_fit_the_retained_history
tests/unit/test_collection_tags.py::test_exact_tag_revisions_expire_with_the_catalog_history_that_names_them
```

`scripts/extent_witnesses.py` already provides scoped candidate bindings on the contract side,
including a Riverhog read-collection group with catalog-sync evidence. Determine the exact
applicable owner/scope there before relocating associations; do not attach all five to every
read surface, invent a new obligation, or label them executed evidence. This bundle does not
perform that relocation or claim its exact owner mapping has been validated.

The intended migration replaces the old implementation-policy register; it is not a permanent
second inventory of the same guidance. After resolving that contract-side disposition, update
the existing consumers together:

- Replace `scripts/implementation_policy.py` and its generated implementation-witnesses JSON
  with the new register/renderer, and replace the old dedicated unit test.
- Update the script/target references in `Makefile`, the existing CI target and its test in
  `.github/workflows/ci.yml` and `tests/unit/test_github_actions.py`, and the qualification
  command reference in `release.toml`. Replace the existing job rather than add another one.
- Remove the old qualification-input entry and old projection-consumer block from
  `tests/unit/test_qualification_fixtures.py`. The new source is intentionally outside
  `qualification/`; its actual consumer is checked by `GuidanceRepositoryTests`.
- Add a plainly labelled non-binding-guidance link in `README.md` and explain its limited
  role in `AGENTS.md`. Link to the register rather than copying its heuristics into another file.

Suggested Make targets, using the repository's existing command wrapper:

```make
.PHONY: guidance guidance-update

guidance:
	$(call UV_CMD,python scripts/guidance.py check)

guidance-update:
	$(call UV_CMD,python scripts/guidance.py update)
```

A failing guidance check means malformed bookkeeping, a stale rendering, or a broken test
reference. It is not a finding that an implementation violated a binding heuristic.
The register does not decide which behavior tests must pass for a release.

Guidance-only edits should leave authoritative contract semantics unchanged. Do not blindly
refresh the contract baseline to make a guidance migration pass. A new commit or changed
source provenance may legitimately change provenance identities; that is not permission to
change contract meaning.

## Drift resistance and its limits

The checker rejects unknown fields, malformed IDs, duplicate IDs, duplicate links within an
entry, empty required prose, unsafe paths, unresolved test symbols, and stale generated
Markdown. It permits one test to relate to multiple heuristics. It writes only the fixed
guidance README path and rejects symlink redirection of the register or output.

The page is deterministically derived from the sole editable register. Links omit line
numbers to avoid churn when unrelated lines move. The exact function/method name is visible
in the link label. Each section is labelled non-binding, including when reached by a deep link.

The repository regression generates fresh contract projections in two subprocesses. In the
second, Python audit hooks deny guidance imports and reads; the projections must match and no
attempted dependency may be recorded. This exercises the current Python generation path, not
every hypothetical external consumer. Contract consumers outside that path would need their
own boundary check. This test is supplied but has not been run in the full checkout.

A stable test name can outlive the behavior that made the association useful. No format or
AST checker establishes the continuing truth of arbitrary prose, human adoption, or adequate
behavior coverage. Review scope and relation notes when relevant tests change. Avoid pretending
that per-test hashes, copied passing statuses, or coverage percentages solve that problem.

## Validation performed and remaining

Executed locally: 32 standalone checker/renderer tests, with 17 parameterized subtests;
all passed. The two full-repository tests were deliberately deselected, not passed or silently
skipped. The seed parses, contains eight records and 35 syntactically valid test references,
and its rendered page is current and deterministic. See `validation-unit-tests.txt` and
`validation.json`. `REVIEW-PREVIEW.md` is a review-only export with test links pinned to the
inspected revision; it is not an additional file to maintain in the repository.

Not executed: all-reference resolution against a complete latest-main checkout, the full
contract-boundary regression, the 35 related Riverhog tests, the existing lint/unit suites,
release qualification, or GitHub Actions. GitHub source reads succeeded, but this runtime
could not clone the public repository because its network DNS lookup failed. No remote files,
branches, issues, pull requests, or repository settings were changed.

Standalone checker tests can be repeated from this bundle with:

```bash
python -m pytest -q --noconftest tests/unit/test_guidance.py -k GuidanceUnitTests
```

In the complete Riverhog checkout after reviewing and integrating the migration, use:

```bash
make guidance-update
make guidance
make unit TESTS=tests/unit/test_guidance.py
make contract-freeze
```

Then run the repository's normal required validation. The integration changes and
contract-side disposition described above are not applied by this reference bundle.

## Inspected source references

All links below pin the inspected base, rather than a moving branch:

- [Existing witness register](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/scripts/implementation_policy.py)
- [Repository guidance and sources of truth](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/AGENTS.md)
- [Architecture and contract-authority boundaries](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/docs/architecture.md)
- [CLI presentation tests](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/tests/unit/test_cli_json_output.py)
- [Operation qualification tests](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/tests/unit/test_operation_qualification.py)
- [Installation derivation tests](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/tests/unit/test_release_installation.py)
- [Scoped contract-side evidence](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/scripts/extent_witnesses.py)
- [Contract generation](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/scripts/contract_freeze.py)
- [Qualification-input accounting](https://github.com/nashspence/riverhog/blob/e0413a288fe3e817db16d5e887b08622a24b3c94/tests/unit/test_qualification_fixtures.py)
