# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #961; convention: #903.
Audited repository base: main@7aa9997bcf05a50c42d7ecd4767730263a25d381
Audited tree: 6b0cb7a536a768340911b24e9dd92836033a8d5a
Reference branch: reference/external/961-growth-onboarding
Authority: https://github.com/nashspence/riverhog/issues/961#issuecomment-6103986307

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer
Published by: connected GitHub integration; commit metadata is publication metadata.

## Purpose and application

Add only the approved growth-bounds section to the original approved #961 guide.
The repository base does not yet contain that onboarding guide; AGENTS.growth.patch
therefore applies to the issue-approved prospective document, NOT current AGENTS.md.
Implement #961's existing onboarding/rename work, then apply this exact additional
delta before updating its integrity and narrow semantic tests. Preserve all other
approved guide text and links. No declaration of current runtime conformance follows
from landing developer guidance.

The subsequent #963 guide patch must consume this amended document and preserve the
new section. #964 establishes owner-scoped v1 growth declarations and executable
witnesses. Do not move inventories, numeric targets or source-history discussion
into the guide. #960's explicit time-shortfall closeout disposition stays intact.

## Files and verified identities

- AGENTS.growth.patch: exact insertion after Bound the work, not the archive.
- verify_guide_patch.py: stdlib scratch-application check; it never modifies input.

Original approved guide SHA-256:
55f9289fd03100d69c9768790754c87931759cd37b572a100a647f6e4194fcdd
Amended #961 guide SHA-256:
84595617b207ef5a4ed7ae3703d170a662e59a33531dd018b246f2beee9e365a

Run `python .reference/verify_guide_patch.py /path/to/original-approved-AGENTS.md`.
A changed input is a reconciliation requirement, not permission to suppress guards.
Do not copy this reference material onto main as another policy authority.

## Validation actually performed

Verified the original full guide against its previously recorded SHA-256; applied
this patch and the new #963 guide patch in sequence with git apply --check; compared
complete outputs; checked preservation of all unrelated guide sections and links
(the Introduction target uses #961's approved architecture-to-introduction rename).
Four guide checks passed in the combined revision check, and the standalone verifier
passed. Python syntax and applied-document whitespace checks passed. Raw patch
context markers are excluded from reference-text whitespace checks. The old timing foundation's
separate source-extracted checks do not validate this guide's full repo integration.

Not run: locked repository lint/unit/dist/build, installed guide policy tests, Docker,
full qualification or independent GitHub checks. This branch changes only reference
material; the actual integration agent must update the current integrity/semantic
checks and validate the integrated exact source normally.

## Authority and publication

The owning issue and later maintainer decisions control. Reference creation,
validation or publication is not an alternate integration rail or approval to bypass
contracts, protected state, release governance or qualification. The exact published
SHA in the issue is the durable handoff identity. No producer-local commit identity
is claimed to have been published; verify the reconstructed tree before recording it.

The available publisher does not expose native Development linking. Record that
limitation on #961; a Markdown URL does not establish the native relationship.
