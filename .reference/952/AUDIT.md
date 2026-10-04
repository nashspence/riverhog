# One Documentation Audit Record, two reading surfaces

This superseding #952 experiment adds generated drift evidence to the existing
front-matter and prepared-candidate model. It adds no author index, review stamp,
per-finding acknowledgement database, semantic authority or second documentation site.

## Author and reviewer workflow

Keep the equivalent of three human entrypoints in `make help`: documentation-plan,
documentation-preview and documentation-check. Match existing repository conventions
rather than preserve those names as aliases. They request stages of ONE generator.
The terminal, check exit status and shared Contract Render read the same generated
Documentation Audit Record. They never separately infer drift or override findings.

Plan reports missing subjects with readable source names and copyable front matter.
Preview can render incomplete authoring work and clearly unverified native outputs.
Check fails on mechanical blockers; release preparation additionally requires the
prepared stage and complete real destination evidence. No wrapper silently upgrades
preview evidence into prepared evidence. No additional audit/drift/diff/coverage make
commands or mandatory second review ceremony are needed.

## Meaning versus prose

Keep the existing Contract Audit Record unchanged as the source of contract analysis
and qualification. Documentation audit is separate nonnormative evidence about prose
coverage, impact, binding and delivery, not another compatibility promise. Reuse
#950's owned-value/global comparison and native discovery rather than maintain a
second hand-curated semantic inventory.

Use stable source targets and capture their owned semantic value AND applicable
parent/shared/reference/policy context. A parameter default or shared security policy
can change while an element ID stays unchanged. Include source-resolved dependencies,
with deterministic cycle-safe traversal in native integration. If precise scoping is
not available, disclose conservative parent/global scope; never pretend a leaf-only
hash establishes that no relevant meaning changed. Document the fingerprint profile.

Capture the effective selected summary/Markdown sections, canonical donor and linked
guide content separately from source-file location and raw corpus identity. A file
move or front-matter quoting edit can preserve effective prose, but still changes
source provenance and invalidates selection of the old candidate. Arbitrary guide
claims are not proved by its subject list; global changes and undeclared dependencies
still require human judgment. This is change prioritization, not a prose truth engine.

The complete generated delta includes added/removed subjects; meaning-only, prose-only,
and coordinated changes; requirement policy/classification/detail/ownership/donor
changes; destination inventory/projection changes; and global/unattributed changes.
Retain before/after values or exact links to their retained evidence, not only hashes.
Always retain removed and reclassified requirements; a shrinking denominator is not
proof of improving coverage. Show which Markdown document/section and native outputs
are affected. Do not force pointless prose edits just to clear a REVIEW finding.

## Baseline selection and honesty

The trusted coordinator resolves one explicitly identified baseline, ordinarily the
last published same-series product; an explicitly selected earlier prepared candidate
can support iteration but is not called approved without its actual approval evidence.
Record code/docs/policy/profile identities and the baseline manifest digest and verify
its provenance using existing mechanisms. Do not select a moving docs branch or let
the authoring corpus select an easier baseline.

A genuine first release has an explicit no-baseline/initial-review state, not a green
empty diff. Missing/corrupt REQUESTED evidence is an error. No prior release is required
to develop the machinery. Incompatible fingerprint profiles produce unknown comparison
and prominent review/full-delta fallback, never unchanged. Unsupported formats fail
explicitly. Preserve old retained snapshots and policy identities; do not silently
regenerate a historical baseline using today's compiler to hide changes.

The reference's `baseline_sha256` validates byte identity ONLY. Production must verify
baseline provenance and the legitimacy of initial mode. Its input requirement and
semantic-scope ledgers are trusted producer inputs, not evidence of native completeness.

## State and context

PASS: the checks represented at this stage passed with no flagged review attention.
REVIEW: mechanically usable but a meaning/prose/policy delta, initial/no-comparison state
or preview-only evidence needs attention. It is not a claim that prose is false.
FAIL: missing required coverage, invalid/stale source binding, invalid local link,
unsafe input, missing prepared destination evidence or a verified projection mismatch.

Malformed input may stop generation with a structured diagnostic/failed command rather
than fabricate a complete record. Aggregate severity cannot suppress a worse finding.
Every record carries stage, performed checks and per-destination matches/mismatch/
unverified status. A preview without native extraction never labels parity verified.
A prepared candidate with any FAIL cannot reach release approval. REVIEW stays visible
in the exact signed review packet; existing human approval handles it without per-item
waivers. PASS does not establish human reading, approval, compatibility or prose truth.

## Shared renderer, not a competing application

Integrate a Documentation Audit panel/view in the existing Contract Render. At the
index show bounded counts and filters with full details available. At a subject show
coverage, old/new meaning and prose, source document/section, native destination parity
and global attention. Keep the complete contract and canonical navigation unchanged;
Audit, Documentation and Documentation Audit are distinct composable records/views.
Do not multiply application routes or interpret these controls as access control.

The runnable `render_panel` is an escaped no-JavaScript FRAGMENT to exercise the consumer
boundary, not production navigation/layout. It uses the exact report findings also
shown by `terminal_summary` and `check_record`. Native integration still owns accessible
controls, linkable state, real subject routes, browser tests and source-friendly labels.
Do not ship raw reference-target JSON as the production author's main interface.

Retain the exact machine audit with immutable release evidence. Public documentation
need not enable maintainer audit UI by default. Historical pages must not silently
reassess released prose under today's policy; any later analysis is separately scoped.
Hidden HTML is not private: public artifacts and bundled records contain no secrets.

## Acyclic binding and candidate promotion

1. Build/extract native documentation readouts and unaugmented content shards.
2. Compare source scopes, requirements, prose and those observations; produce audit.
3. Render the audit into the review packet using the shared renderer.
4. The enclosing candidate/release manifest binds inputs, artifacts, observations,
   the audit AND the review packet. Existing approval/signing selects those bytes.

The audit does not hash itself, the final enclosing manifest, or the audit-augmented
HTML that embeds it. Those hashes live one level outward. Review HTML cannot claim
independent evidence about itself. Publish the same selected bytes; changed baseline,
inputs, findings, report or review panel requires a new candidate selection.

`audited_candidate_evidence` adds report/panel binding to `candidate.py`'s earlier
low-level identity primitive. `verify_audited_candidate` refuses substitution. The
reference binder does not authenticate a signature, discover all inputs, or prove
observations really came from artifact execution. Production must keep #950's trusted
run/artifact path and actual installed-output extraction, native JCS and manifests.

## Validation scope

69 focused tests pass including all 40 prior witnesses plus 29 audit witnesses.
New cases cover stable-ID meaning drift, shared/global context, guide/prose changes,
requirement relaxation/removal/canonical retargeting, missing children, initial and
unavailable baselines, profile incompatibility, native preview versus prepared evidence,
changed/removed destinations, exact report/HTML/candidate binding, escaping and bounded
summaries retaining the complete record. No generated audit or demo artifacts are
committed. Full native discovery/browser/release integration is still unimplemented.
