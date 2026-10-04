# #952: front-matter authoring and review of prepared outputs

Subordinate to #952 and subsequent maintainer decisions. This revision supersedes
the experiment at `6540d5500361b274192f34ef7d11ab5c71a728e8` without rewriting it.
The audited code base remains `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`.

## Decisions

Source owns meaning; the release corpus owns editorial expression. Preserve source
membership, names, defaults, signatures, encodings, guarantees, diagnostics, legal
facts and useful development context. A guarantee in prose remains source-owned.
Audit mixed fields before relocation; do not strip every `description` key. A
one-time deliberate semantic representation rebaseline is permitted, not loss of
promises. Thereafter prose alone changes artifact/document identities, not Closure.

The corpus supplies release-quality CLI/OpenAPI/Python/package/image prose. It is a
release BUILD input, not a Pages overlay. Source-owned adapters select native slots;
authors cannot supply import paths, arbitrary patches or executable transforms.

The human flow is plan, write Markdown, inspect a prepared candidate, approve/promote
its exact artifacts. The previous central JSON index and committed review.json are
removed. A generated manifest is not an authored registry or proof of human reading.

## One metadata location: the Markdown document

One orphan release-documentation branch contains a complete subtree per release.
Scan every permitted .md file in that subtree in sorted order. Each has one leading
--- YAML block, one stable document ID and a reference or guide kind. The schema in
source.schema.json describes the PARSED front matter, not an authored JSON file.
No file inventory or version is repeated in a central corpus config. Metadata is
excluded from displayed prose. Long prose remains the Markdown body.

A reference document's subjects pair exact targets with short summaries. An optional
body selector chooses its complete body (`#`) or a named heading section (`#count`).
A selected section includes subordinate headings and stops at its next peer/ancestor.
Omitting body means summary-only; do not attach a whole page to every member by
accident. A guide lists relevant subject targets but grants no implicit coverage.
Multiple related subjects can share one file; no per-scalar document mandate.

Document identity is independent of path. Moves preserve binding identity but change
captured source identity. Duplicate document IDs or authoritative subject bindings
fail. An unlinked valid document is included, not silently dropped as an orphan.
Unexpected files and Markdown without valid front matter fail this reference profile.

The runnable parser uses PyYAML BaseLoader node composition, string-only scalars and
explicit token/schema checks. It rejects duplicate/merge keys, anchors, aliases,
explicit tags/directives, complex keys and unsupported metadata. All prose/identity
scalars remain strings: on, false and dates do not undergo implicit type conversion.
Header byte and nesting budgets are operational protections. This is a constrained
application profile over a real YAML parser, not a custom YAML interpreter.

Targets retain native element identity plus a verified object-member pointer or
named member. Named CLI/Python members are not parameters/0. The NATIVE producer
must establish ownership and slot eligibility; JSON Pointer existence is insufficient.
The prototype consumes an already-trusted obligation ledger, not a user inventory.

## Requirements without author bookkeeping

Source policy derives authored obligations, genuine canonical reuse and structural
facets using the existing registry/discovery/owner maps. Account for all meaningful
members across the current interface families; no blanket self-describing waiver,
inferred aliases, legacy-text auto-pass or parent paragraph covering descendants.
A schema's type bit is a generated facet, not an excuse to require prose boilerplate.

`documentation_plan` exposes missing/covered/structural subjects with their native
authority/interface and exact targets. Production should add friendly names and
optional explicitly requested starter files with no invented prose. It must not
rewrite existing docs or treat empty starters as completion.

`compile_corpus(..., final=False)` permits incomplete previews and reports missing
subjects. `final=True` requires complete structural coverage, NOT human approval.
There is no proposed_review API or review.json input. Source/compiler/policy/corpus
changes are bound through generated preparation evidence and invalidate a selected
candidate, rather than requiring an author to refresh committed hash stamps.

## Markdown safety and destinations

Use the pinned Markdown parser. The experiment recognizes then rejects raw HTML and
unsafe schemes, validates parsed links and fragments, and treats fences as data.
Logical heading IDs are deterministic; emitted DOM IDs are prefixed doc- to avoid
uncontrolled document names. Body selectors and ordinary Markdown links use logical
heading slugs; the compiler maps them to emitted IDs. Duplicate headings fail.

The runnable subset supports Markdown only, not binary images/assets. Production
must implement the issue's explicit safe asset policy and source ledger without
silently omitting selected inputs. Exact original bytes include front matter and
line endings; compiled HTML/plain text is a separate product.

The prototype's intermediate HTML is not a second production site. Integrate its
concepts with the existing Contract Render and source-owned routes. Native terminal,
OpenAPI, Python and package destinations need context-correct links and deterministic
projections; a Pages-relative path is not automatically an offline native link.

## One prepared candidate, not two approval ceremonies

Use optional cheap authoring previews, followed by the existing nonpublishing release
preparation. Stage approved documentation resources/metadata before building actual
artifacts. Read prose BACK OUT of the installed wheels/apps/images and compare each
destination to its expected projection. Generate a browsable review index containing
those observed readouts, the rendered documentation, source links and coverage.
A UI made solely from the compiler's expected strings is not native-output evidence.

The candidate manifest binds exact code and docs commits, full source-file ledger,
semantic Closure, requirements/policy, compiler/toolchain, compiled record, actual
readouts, review packet and artifact hashes. Reuse existing release manifest/checksum
mechanisms. Keep signatures external to the bytes they approve; no self-hash cycle.

The maintainer reviews THAT prepared candidate. Existing protected/offline release
approval selects and signs its exact asset set, including reviewable evidence. Prefer
promotion of the same bytes without rebuilding. A changed input, output, artifact or
review packet requires a new candidate/review; rebuilding cannot silently reuse an
old approval. Destination representations differ intentionally, but each actual
released destination must equal its own reviewed projection.

`candidate.py` demonstrates binding and stale/altered candidate refusal. Its selected
argument is trusted caller input; it is NOT an authentication/signature API. Tests use
real argparse readouts but synthetic artifact bytes, NOT built Riverhog wheels/images.
The helpers cannot prove an observation came from an artifact: production must own
that extraction step. Nor can hashes prove truthful prose or that a human read it.
Independent tests, executed journeys and human semantic judgment retain their roles.

## Native integration map and retained boundaries

| Existing seam | Required integration |
|---|---|
| documentation.py and branch validator | Exact subtree capture, front matter, safe Markdown/assets and complete source ledger |
| discovery, owner maps, INTERFACE_REGISTRY | Complete native requirement and member-slot producer; no hand-maintained API registry |
| contract_freeze, structural_json_schema | Explicit mixed-field disposition and semantic rebaseline, no broad key removal |
| cli_documentation and native CLI/API hooks | Compiled prose into actual destinations; preserve parsing/schema/signatures |
| prepared_source and build_release_evidence | Bounded transforms BEFORE backend builds; self-contained sdists/resources and metadata parity |
| release manifest and workflow_evidence | Bind exact code+docs+compiler+prepared artifacts and actual readouts; preserve trusted coordinator separation |
| Contract Render and Pages | One browsable prepared review packet; immutable historical render/index assembly and verified-byte copying |
| existing protected approval/offline signing | Select and promote exact reviewed candidate; no new approval registry |

Native helpers remain small demonstrations: argparse, operation-level OpenAPI, owned
Python functions and preparation of a metadata table. Click, nested/shared schemas,
immutable Python objects, OCI, package-local loading and actual sdist/wheel/image builds
remain integration work. Respect static/dynamic metadata rules and never patch a built
or signed wheel. Independent recovery/components must not acquire a monorepo docs
runtime. Keep useful development fallbacks, but reject them as final prose coverage.

Keep #950's exact upstream evidence, installation indexes, source-vs-coordinator split,
offline signing, whole-site freshness, capacity controls and history custody. Actual
v1 prose and publication remain #440/#444 work. No history or release ref changes here.

## Finite handoff

Implement ownership migration, native requirements, front-matter compiler, adapters,
prepared review packet and exact promotion checks through normal integration. The
reference's serializer is not Riverhog JCS; use native canonicalization in production.
The reference ledger's complete=true is a trust boundary, not completeness proof.
Do not copy the branch workflow as a new permanent gate or mechanically merge this
experiment. No corpus/CMS framework, invented compatibility layer or automated prose
truth certification is requested. Human reading and executable journeys retain their roles.
