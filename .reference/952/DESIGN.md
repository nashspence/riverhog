# #952: meaning, prose, and compiled publication

This reference is subordinate to issue #952 and subsequent maintainer decisions.
It is a concrete design/compiler experiment, not a production patch or complete
coverage extractor. Base: `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`.

## The dividing point

A sentence can be a contract. A string in executable metadata can be editorial.
Classify by whether changing it changes promised behavior/identity or explains the
same promise, not by the filename or the spelling `description`.

Source remains authoritative for membership, types, units, defaults, bounds,
security and recovery guarantees, legal facts, dependency/ownership relationships,
identifiers, diagnostics and operative examples. Keep useful maintainer comments,
internal docstrings and the terse main architecture map. Never move a guarantee
only into optional documentation just because it was embedded in a help string.

The release corpus owns human summaries, usage explanations, native help prose,
OpenAPI annotations, public Python reference prose and editorial package/OCI text.
The prose must be projected into the actual shipped surfaces, not added only to
Pages. Normal main development keeps syntax, meaningful diagnostics and visibly
unreleased terse context; development fallback is not accepted release copy.

This revises #950's minimal authoring restriction without undoing its source-only
storage, immutable products, installation index, authority fencing, or history work.
Do not rewrite #950 or revive a generated-content branch. Full v1 prose and release
selection remain #440/#444 work, not a prerequisite to this machinery checkpoint.

## Three identity domains and one acyclic pipeline

1. Code/source semantics: exact code SHA and source-derived semantic Closure.
2. Authored inputs: exact docs commit and selected version subtree, with an exact
   path/byte digest ledger. Whitespace changes are source changes, not silently
   normalized away. Review state is separately identified.
3. Compiled/built outputs: policy/compiler/toolchain, normalized Documentation Record,
   native resources, metadata, packages/images, Render and qualification identities.

Discover meaning and documentation obligations before consuming prose. Then compile
prose and prepare source, build artifacts, rediscover/check the same semantics from
installed artifacts, validate their actual documentation, and qualify/sign/publish.
Code-only qualification is insufficient for prose-injected runtime artifacts. A
signature cannot be an input required to build the very bytes it approves.

Current prepared-source proof permits version and lockfile preparation. Add an exact
allowlist of deterministic documentation resource/metadata transformations, not a
blanket dirty-tree exception. The produced sdist must rebuild its documented wheel
without Git or another branch. Stage static packaging metadata BEFORE the backend
reads it, or use a deliberately supported dynamic field; never override a declared
static field inside a backend or patch built/signed wheel metadata afterward.

A change to artifact descriptions can change wheel/image hashes without changing
semantic Closure. The current projection includes some package/authority prose, so
first make and record the narrow representation rebaseline. Do not demand the old
hash survive, and do not strip every key named `description` to force invariance.

## Authoring contract

One orphan `release-documentation` branch; one self-contained subtree per version.
No code/history merges, executable build files, symlinks, or cross-version file
includes. The exact selected commit is an input; branch tips are navigation only.

`documentation.json` is an index, not a giant escaped book. The tested subset is in
`source.schema.json`: entries bind a target to a one-line plain summary and optional
Markdown body; guides bind a Markdown body to explicit existing subjects. JSON is
the sole ownership metadata location; no parallel front matter is required.

A target is either:

```json
{"element_id":"<real existing id>","pointer":""}
```

or an exact object-key member pointer validated by the native owner map, or a
named member such as:

```json
{"element_id":"<real CLI command id>","member":{"kind":"cli-parameter","key":"host"}}
```

The typed member form avoids binding an option to `parameters/0` when insertion or
reordering changes that array. A source-owned resolver joins the stable native name
to its current exact witness. Python parameter/result selectors follow the same
principle. Extending member kinds is a source-policy change, not arbitrary author
input. A JSON Pointer's existence alone is not ownership or a documentation slot.
The reference compiler consumes a trusted already-resolved requirement ledger; it
DOES NOT establish native pointer ownership or extract the complete inventory.

Production may support explicitly listed flat entry shards and safe raster assets.
The runnable subset deliberately refuses images, shards and unknown files until
those policies are implemented. Do not mistake that subset for a mandate to inline
long text. The issue defines the complete integration outcome. Do not add a custom
Markdown framework, recursive includes, arbitrary JSON patches, or executable data.

## Requirement policy and review

The exact source derives the set of obligations using the existing registry,
Closure owners, native parser/signature structure and schema traversal. All 23
current interface families need dispositions; new kinds fail classification.
A command root is not a substitute for all its parameters. A public schema/export
root is not a substitute for meaningful members. Conversely, a schema's `type` and
`required` bits need generated representation, not separate made-up paragraphs.

The producer marks each obligation authored, a real canonical reference, or a
structural facet. Authors cannot set `self_describing`, inject defaults, invent an
alias, or cover unknown subjects. Canonical reuse must follow a verified source
relationship and remain contextually accurate. Equal types or similar names do not
prove equal meaning. A shared guide may explain multiple explicitly bound subjects;
it does not implicitly waive all descendants.

The compiler reports every unresolved obligation and accepts incomplete previews.
A final build rejects unresolved requirements. Native destination parity and actual
journey execution are additional gates: structural coverage is not prose correctness.

Keep review state small. `review.json` binds semantic Closure, requirement ledger
(including policy) and corpus byte-ledger identity. It excludes itself from the
corpus digest to avoid a cycle; its bytes remain in the full source ledger/archive.
`proposed_review()` is an explicit review aid and requires complete coverage.
Ordinary builds never create or refresh it. Editing meaning, requirements or source
prose invalidates a copied record. It is NOT a signature or proof of human review;
the existing protected/offline release approval approves the resulting exact assets.

The reference's report serializer and `*-reference/v1` identities are not new
production canonical formats. Integration must use Riverhog's native RFC 8785
codec and manifest mechanisms. The supplied `complete: true` fixture is a trust
boundary: a production coordinator must derive it, not accept it from the corpus.

## Markdown and destinations

Use the pinned markdown-it-py parser. The experiment parses Markdown ASTs, refuses
raw HTML and unsafe schemes, validates local files/fragments, and never executes
fences. It captures original bytes independently from compiled HTML/plain text.
It rejects heading collisions, case-colliding paths, control sequences and orphans.
Reference links ending in `.md` become `.html` in the intermediate HTML; original
Markdown stays unchanged. Production must resolve links through the EXISTING
Contract Render, not publish this experiment as a second application.

`contract:<percent-encoded existing element ID>` is a compile-time link. The trusted
renderer supplies its actual route; missing subjects/routes fail. The corpus cannot
invent those routes. Exported OpenAPI, terminal help and package descriptions need
explicit context-appropriate links, not an assumption that a Pages-relative href
works everywhere. Native consumers must not fetch a moving branch or full corpus.

The native seams here demonstrate literal argparse help (including percent-format
hazards), operation-level OpenAPI prose, owned Python docstrings without wrappers,
and preparation of a new project metadata table. They are NOT exhaustive adapters:
Click, nested/shared schemas, compiled/immutable Python exports, wheel builds, OCI,
package-local resource loading and cross-context link projection require integration.
Independent recovery/components must not acquire a monorepo documentation dependency.

## Actual migration map

| Existing seam | Required change | Evidence |
|---|---|---|
| `contract_atlas/documentation.py` authoring-tree validator and `AuthoredDocumentation` | Resolve a pinned subtree/file ledger, compile v2 index plus Markdown, carry source bytes and review fence | Dirty checkout cannot alter selected content; missing/extra/unsafe paths fail |
| `contract_atlas/cli_documentation.py` | Replace release parser-prose extraction as editorial authority with a compiled prose projection; retain native syntax/discovery | Installed Click and argparse help match selected prose while parsing/defaults are unchanged |
| `contract_freeze.py`, `model.py::structural_json_schema`, discovered owner maps | Field-family semantic disposition and real member obligations | Property named `description` survives; semantic promises never disappear |
| `model.py::INTERFACE_REGISTRY`, member discovery | Closed requirement profiles and canonical-reuse witnesses | Every interface/member classified; new kinds and missing children fail |
| `generation.py::prepared_source`, `build_candidate` | Exact multi-input preparation proof and generated resources before release build | Only approved transforms; reproducible prepared source and installed semantic parity |
| `release.py::build_release_evidence`, package `pyproject.toml`, Docker build metadata | Stage editorial fields/resources before artifacts are built; archive inputs | sdist-to-wheel/offline parity; metadata, licenses, coordinates and dependencies remain correct |
| `html_rendering.py`, `publication.py`, `contract_pages.py` | Consume compiled record and full exact corpus archive through existing pipeline | Documentation/Audit modes and immutable historical bytes; installation indexes retained |
| qualification/preparation/publication workflows and `workflow_evidence.py` | Qualify the exact code+docs+compiler+prepared artifact tuple | A previous code-only or different-docs run cannot authorize the new assets |
| documentation authoring workflow and policy tests | Preview/coverage/review command plumbing on trusted main coordinator | No corpus code execution or automatic review stamping |

Current qualification coordination is separate from selected source after #950's
last fix. Preserve that distinction: the coordinator is not imported accidentally
from a historical source checkout. Retain whole-site freshness, installation-index
verified-byte copying, offline signing and upstream-run provenance checks.

## Ordered slices and finite stopping point

1. Meaning/prose field disposition, precise rebaseline and development fallbacks.
2. Exact subtree compiler, Markdown/index format, safe links and source archive.
3. Complete native requirement producer, reports and explicit review binding.
4. Native resource/metadata adapters and deterministic prepared-source proof.
5. Built-artifact semantic AND documentation parity; qualification ordering.
6. Multi-version synthetic publication/installation/site witnesses, then standard
   integration gates. No actual v1 prose/publication is necessary for this issue.

Do not merge the reference wholesale. The core compiler experiment tests the join,
not the truth/completeness of the producer. No universal proof of prose correctness,
per-scalar bureaucratic inventory, arbitrary source-patch engine, new ontology or
parallel CMS is requested. Human reading and executable journeys retain their roles.
