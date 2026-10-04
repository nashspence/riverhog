# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #952. Convention: #903. Predecessor: #950.
Audited code base: main @ `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`.
Base tree: `079c49f5ea400d891d2ee5e6453a38afebfb51b9`.
This revision is a child of prior reference `dac22c6d3281e9edc5575fee094338a5855eb074`.
It supersedes that handoff with generated documentation drift/audit evidence, preserving
its front-matter and exact prepared-candidate design and all prior reference ancestry.

Produced by: OpenAI ChatGPT.
Model: GPT-6 Astra Pro (user-visible identity).
Configuration/effort: not exposed; no hidden configuration claim.
Requested by: Nash Spence, Riverhog maintainer.
Published by: OpenAI ChatGPT through the connected GitHub tool on the maintainer's behalf.
Date: 2026-10-03 (maintainer timezone).

## Purpose and contents

Use Markdown front matter as the single authored identity/binding location. Remove
central documentation.json and committed review.json from the proposed authoring model.
Keep source-owned meaning and complete discovery-derived obligations. Make the human
trust stage inspection of ONE actual prepared candidate, approved by the existing
protected/offline release mechanism and promoted without unnoticed rebuilding.

Start with `952/DESIGN.md`, then `952/AUTHORING.md` for a tested human-facing example.
`compiler.py` supplies a strict YAML/Markdown reference compiler and coverage plan;
`source.schema.json` describes parsed front matter, not an authored JSON index.
`candidate.py` is the earlier low-level candidate-binding primitive.
`documentation_audit.py` adds shared report generation, scoped meaning/prose/policy
comparison, phase-aware checks, an HTML fragment and audited candidate binding.
Read `AUDIT.md` for baseline/dependency scope, state semantics and production limits.
Tests exercise these helpers plus small native projection seams. RESEARCH.md records
primary references and integration consequences. The branch-specific workflow is an
isolated reference check, not another permanent production gate.

## Validation actually performed

Verified the complete downloaded clone at the prior handoff SHA, including all 1,369
exported file sizes/SHA-256s/modes, exact HEAD/tree and `git fsck --full` before edits.
Archive SHA-256: `5c01faec347d035c3c732cde955d5aad57d2e15841551940b1b76becf95271f8`.
Manifest SHA-256: `fe71d923ac2d7e5da2de439666f83cd212e827b87647d68c1a58e8c1bb90c071`.

69 isolated tests passed locally with markdown-it-py 4.2.0, PyYAML 6.0.3 and
jsonschema 4.26.0. Compilation and diff whitespace checks passed. The owning issue's
superseding handoff comment records exact published commit/tree and hosted results.

Witnesses include strict YAML refusal, string-only scalar interpretation, missing
child obligations, exact member sections, duplicate IDs, file moves, valid unlinked
pages, source-only canonical reuse, unsafe Markdown/links/control sequences, exact Git
capture despite a dirty checkout, literal argparse percent and unchanged parsing,
OpenAPI wire fields named description, unchanged Python callable signatures, technical
metadata preservation, and invalidation of changed candidate inputs/artifacts/readouts.
AUTHORING.md's two front-matter examples compile against the synthetic requirement set.

## Not validated / limitations

This is an experiment, not a completed production patch. Complete native requirement
extraction and owner/slot validation remain integration work. An input ledger saying
complete=true is not proof of completeness. The helpers do not prove the source
producer's truth, human prose accuracy or human approval. Audit snapshot ownership,
semantic dependency completeness and baseline authenticity are native-coordinator
responsibilities; the reference supplies synthetic witnesses, not a production extractor.

The runnable profile captures Markdown only; binary assets/images remain refused.
Intermediate HTML is not another production site. Native seams cover argparse,
operation OpenAPI, owned Python functions and a metadata table, not full Click/shared
schema/OCI/resource/sdist/wheel integration. Candidate tests use actual argparse output
but synthetic package bytes. The candidate/audit helpers validate binding and mechanical findings only; production
must extract observations from real artifacts and use the real signing/approval path.
Reference serialization is not Riverhog JCS. Reuse native codecs and release manifests.

Full locked Riverhog generation, lint/unit/dist/build, browser/platform/image/provider
and release qualification were not run for this additive reference. Focused tests do
not replace them. No production callers, protected branches, release-documentation
corpus, release/tag, Pages deployment or history custody were changed.

## Authority

Creation, testing, publication, requester authority and native issue linkage do not
make this branch authoritative or authorize integration/release. The current #952
issue and subsequent maintainer decisions control. Reconcile against current source
and integrate through the normal rail; do not mechanically merge the experiment.
The exact published reference SHA in the owning issue is the handoff identity.
Preserve prior handoffs; retire navigation refs only after owning-issue disposition.
