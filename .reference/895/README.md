# #895 reference: Closure / Audit boundary and shared HTML

**Non-authoritative external reference under #903.** The current #895 decision and Riverhog's integration rail control. This is a contained implementation experiment, not the replacement production artifact, a complete migration, or an accepted freeze. See [HANDOFF](../HANDOFF.md) for provenance and validation limitations.

Base: `main` at `d4f944ca350810dfb14377ffd9cf77dcd06468f0` (configuration/YAML convergence included).

## What works in this slice

`legacy.py` reads exact selected records from the existing machine format, or the small checked fixture, into `records.build`. That produces a standalone reference Closure and a separately bound Audit Record. `render.py` uses one contract-body function for both contract-only and Audit Mode HTML. No production generator, release file, existing output, or workflow is replaced.

The source-value fixture contains 15 records: the three real application-access Python examples named in #893, the actual Gogurt route regex affected by #888, CollectionTag, all nine current compatibility promises, and the schema-bound extent rule. Five selected values match their current generated `exact-contract-value` SHA-256 markers. Fixture-only IDs and analysis records are labelled; they are not a second production registry. The legacy adapter instead retains the actual discovered identities. Its default selection is a demonstration selector, not a proposed contract membership policy.

The prototype preserves module/member nesting using the recorded `module`, `unit` and `owner`. It displays the recorded Python kind and complete short compatibility promise directly in inventories. Literal text is escaped once for HTML, never passed through Markdown link escaping. Internal links are real HTML links; punctuation inside a value cannot turn into navigation. No domain/topic classification is added.

## Field roles and extent ownership

These are explicit **reference mapping choices**, not a claim that all repository field roles have already been settled:

- Python identity/structure, signatures, dataclass fields, aliases and constants remain exact. Signatures are not parsed or evaluated. The recorded public distribution and explicit member owner are validated, not guessed from names.
- At actual schema nodes only, `title`, `description`, `$comment` and `examples` are retained as source annotations in audit. Identically named object properties or literal default/enum/const data are not removed. The current `structural_json_schema` function in `scripts/contract_atlas/model.py` already makes a schema-node/prose distinction; this reference extends that distinction explicitly rather than pretending annotations cannot carry an unresolved promise. Production integration must review any annotation carrying unique normative meaning before moving it.
- In the inspected `contract_max` extension, the policy marker stays with its exact bound and the rationale moves to audit. The known CollectionTag reason is explanatory. Other extent policies, extra extension fields, and potentially scope-bearing reasons do not silently fall through: unsupported mappings fail for integration review.
- In the inspected schema-bound rule, `policy`, `authority` and `exceeded` stay; the requirement that a maintainer provide a reason moves to audit as an authoring requirement. Other rule forms are outside this slice.
- Generated bound-decision records are audit analysis, not another authoritative maximum. They must resolve to exactly one selected owned field. Their copied bounds are checked against that field's actual keywords. Missing/ambiguous subjects and stale copied limits fail. There is no name-based capacity assignment, absent-maximum-to-unlimited conversion, or unbound-analysis-to-new-contract fallback.
- Each moved field retains its exact pointer, role and value. Restoring it into the normative value must reproduce the original exact-value digest and reapplying the partition must produce the same normative value and moved fields. Audit cannot overwrite a normative field. These checks account for each selected value, not merely matching aggregate counts.

The Closure's `address` is a logical contract-document address used to interpret scoped references. It is not a source checkout path or a claim that a generator's file layout is immutable. This reference keeps a single normative value per selected identity; the original composite is recoverable only for auditing the partition. Filesystem paths, source lines and original-value digests live in audit.

## Binding and dependency direction

The reference uses the existing `riverhog_canonical_json` package for identity JSON. Large integers use the existing decimal-string plus exact-pointer convention; its interpretation data is inside the relevant value envelope. Duplicate JSON keys, nonfinite values and inexact numeric input are not silently accepted.

The Audit Record names the canonical SHA-256 of the complete reference Closure and its source revision. Its fields, analyses and witness associations resolve to exact Closure subjects. The Closure needs neither audit nor HTML to validate. `manifest.json` hashes outputs but not itself, so there is no self-referential manifest hash.

Changing source lines, rationale-only text, annotations, analysis identifiers or audit records cannot change the Closure or its contract body. Changing a declaration changes the Closure and invalidates an old audit binding. Matching that digest does not prove a test ran against any implementation: the slice only preserves recorded witness context and does not manufacture results.

Witness associations come from explicit decision-ID links. They are rendered as candidate audit context, not contractual guarantees. Synthetic tests exercise affected-row propagation and deduplication; the shipped source-value fixture deliberately contains no fabricated behavioral witnesses. Full claim-to-guarantee migration, as opposed to retaining honest legacy audit context, remains integration work.

## Running in the prepared repository Python environment

The reference imports the repository's existing `riverhog_canonical_json`; it adds no production dependency. Run these with that package and its normal dependency installed. `PYTHONPATH` below selects the checked source explicitly, not a second implementation.

```sh
PYTHONPATH=packages/riverhog-canonical-json/src python -m unittest discover \
  -s .reference/895 -p test_reference.py -v

PYTHONPATH=packages/riverhog-canonical-json/src python .reference/895/legacy.py \
  --out /tmp/riverhog-895-preview
```

The output directory must be new or empty. It contains `closure.json`, `audit.json`, an acyclic output manifest, ordinary linked HTML pages, shared CSS and a small Audit Mode script. Open `index.html` or serve that directory locally. All contract content is generated before the browser loads; no browser-side discovery or model interpreter is involved. Audit state uses `?audit=1` and preserves the fragment. The base contract remains readable without JavaScript. Modes are not access controls.

To exercise the adapter against the checked production input:

```sh
PYTHONPATH=packages/riverhog-canonical-json/src python .reference/895/legacy.py \
  --machine qualification/contracts/riverhog-v1.json \
  --source-revision d4f944ca350810dfb14377ffd9cf77dcd06468f0 \
  --out /tmp/riverhog-895-current-selection
```

Use repeated `--element-id` values to request another exact selection. No source checkout is imported/executed to invent new facts. The selection must contain the normative reference targets and explicit Python parents it needs; the adapter does not repair a missing selection by guessing. The production file invocation above was **not run here**; the adapter was tested on the corresponding legacy structure built from the inspected fixture.

Optional browser checks require Playwright and Chromium in the review environment, not as production runtime dependencies:

```sh
PYTHONPATH=packages/riverhog-canonical-json/src python .reference/895/check_browser.py \
  --chromium /path/to/chromium --report /tmp/895-browser.json
```

The normal runner tests intercepted static URLs, controls and history. `--dom-only` runs real DOM/literal/layout and display-function checks where navigation is restricted and explicitly does **not** claim URL/history coverage.

## Current validation and deliberate limitations

The performed checks and selected-module environment are described in HANDOFF and `validation.json`. Final tests used byte-verified exact source for the repository canonical module and RFC8785 v0.1.4, not reconstructed helper implementations. This is still not the normal full Riverhog workspace suite or GitHub Pages acceptance. Existing dependency sources are deliberately **not vendored** in this branch.

Bounded omissions: no whole-monolith extraction, all-type coverage, new policy applicability engine, full segmented/no-total-max semantics migration, multi-pointer element adapter, external/anchor/dynamic schema-reference resolver, Documentation Record or authored docs, publication, release acceptance, or compatibility engine. Internal HTTP and Python-local references are supported and tested. Unsupported current field-role combinations fail explicitly; this does not assert the full legacy default selection will pass before reconciliation.

The handoff intentionally uses an isolated `.reference/895/` module set and reference-prefixed formats. It is not a request to add this directory or these format IDs to the supported product. Reuse/refactor the demonstrated boundary and output logic into the current implementation only through the integration agent's explicit reconciliation.

## Integration starting points

- Preserve the existing canonical-number/parser package rather than add another hashing algorithm.
- Carry the partition/restore and exact-subject regression techniques into the real builder before moving all fields. Review each remaining structural field family, not a list of individual instances.
- Use current discovered interface/ownership relationships for every migrated renderer. Extend contract-body coverage while reusing the recorded-kind and short-promise comparison treatment.
- Keep the contract-only body independent of audit input. Complete exact claim applicability and source/build evidence checks; move audit context without manufacturing new promises.
- Rerun the source-value hashes and all tests in the actual workspace, then full-closure/identity and delivery checks. Perform the maintainer reading review separately. Neither this branch nor green reference tests closes #895, #888 or #893.

## Pinned sources inspected

All paths below are from the base SHA above, not a changing release candidate:

- `AGENTS.md`, `README.md`, `release.toml` (including the nine compatibility declarations).
- `scripts/contract_atlas/model.py`: `structural_json_schema`, exact-value encoding and semantic identity.
- `scripts/extent_contract.py`: rule definitions and generated extent interpretations.
- `packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py` (blob `8c25c884c893134d57cfb1db7795bcc5867240d3`).
- `qualification/contracts/riverhog-v1/authorities/riverhog-application-access/python/`: ApplicationAccess, MonthlyDownloadQuotaBytes and access_covers exact owned values.
- `qualification/contracts/riverhog-v1/authorities/gogurt-core/python/gogurt-core-gogurt-route-pattern.md` and its source constant in `some-implementations/gogurt/packages/core/src/gogurt_core/mounts.py`.
- `qualification/contracts/riverhog-v1/authorities/riverhog/http-schemas/schemas-collectiontag.md`.

Issue #903 supplies publication/provenance convention; the current accepted tops of #895, #888 and #893 supply the requested scope. Historical layouts are not extra acceptance gates.
