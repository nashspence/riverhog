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

Add only the approved growth-bounds section to the **current corrected #961 guide**.
The repository base does not yet contain that onboarding guide. The currently
approved #961 guide includes the maintainer's pre-v1 hard-cut correction
([#961 comment 6112662156](https://github.com/nashspence/riverhog/issues/961#issuecomment-6112662156)).
AGENTS.growth.patch applies to that corrected prospective guide, NOT the older
guide snapshot and NOT current AGENTS.md. This patch adds no transitional
pre-v1 reader or migration but **retains the intended Alembic v1 baseline,
explicit upgrades, forward-only framework and synthetic revision tests**.
Implement #961's existing onboarding/rename work, then apply this exact additional
delta before updating its integrity and narrow semantic tests. Preserve all other
approved guide text and links. No declaration of current runtime conformance follows
from landing developer guidance.

The subsequent #963 guide patch must consume this hard-cut guide and preserve the
new section. Preserve Alembic initial upgrade-to-head, forward revision machinery
and tests; do not preserve prior pre-v1 layouts, transitional readers or adapters.
#964 establishes owner-scoped v1 growth declarations and executable
witnesses. Do not move inventories, numeric targets or source-history discussion
into the guide. #960's explicit time-shortfall closeout disposition stays intact.

## Files and verified identities

- AGENTS.growth.patch: exact insertion after Bound the work, not the archive.
- verify_guide_patch.py: stdlib scratch-application check; it never modifies input.

Current corrected #961 guide SHA-256:
b954c2965509ce7e69361dd6663646f8920ed5d707733e671ab2425d7c77cd4b
Corrected guide with growth section SHA-256:
1fe4e9a92c4df98949dfe9b52d9d495f186281502f348151491950b72325d39c
Old guide/hashes are historical only, not an alternate accepted input.

Run `python .reference/verify_guide_patch.py /path/to/current-issue-approved-AGENTS.md`.
A changed input is a reconciliation requirement, not permission to suppress guards.
Do not copy this reference material onto main as another policy authority.

## Validation actually performed

The previous validation of the original guide is historical. This revision's
corrected guide SHA-256 and patched output were independently recomputed from the
current #961 issue-approved text; the growth and post-onboarding patch hunks were
applied in unique exact context, preserving unrelated guide sections and links
(the Introduction target uses #961's approved architecture-to-introduction rename).
Historical four guide checks and the previous standalone verifier result remain
source-pinned. For this correction, exact approved-guide input/output hashes and
unique-context guide patch application were rechecked; the new standalone verifier
and installed documentation tests must be rerun on the integrated exact source. Raw patch
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
