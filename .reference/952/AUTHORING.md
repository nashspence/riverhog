# Authoring and reviewing a release

This is an illustrative workflow, not additional production commands. The runnable
examples use synthetic subject IDs from test_compiler. Real IDs and member names
come from the generated documentation plan for the exact code candidate.

## Write beside the binding

The plan tells you which public subjects still need prose and supplies their exact
targets. Open the appropriate Markdown file. Edit its summary and explanation together;
there is no central documentation.json to synchronize and no review.json to commit.

`reference/recover.md`:

```markdown
---
format: riverhog-release-documentation-document/v1
id: recovery-reference
kind: reference
title: Recovery
subjects:
  - target:
      element_id: command
      pointer: ""
    summary: Recover one object.
    body: "#"
  - target:
      element_id: command
      member:
        kind: cli-parameter
        key: count
    summary: Number of objects to recover.
---
# Recovery

Use the generated command syntax. Follow the [steps](../guides/recover.md#steps).
```

`guides/recover.md`:

```markdown
---
format: riverhog-release-documentation-document/v1
id: recovery-guide
kind: guide
title: Recover an object
subjects:
  - element_id: command
    pointer: ""
---
# Steps

1. Select an object.
2. Follow the [reference](../reference/recover.md#recovery).
```

The guide's subject list means "relevant to", not "all children are documented".
A summary-only member needs no body selector. For detailed member prose, add a normal
heading in its reference file and use `body: "#heading-slug"`. That selects just the
heading section, not unrelated sections. File layout is your choice; IDs are stable.

All front-matter scalars are strings. Quote empty pointers and values containing
YAML punctuation. There are no YAML aliases, tags, merges or executable directives.
Every Markdown file in the selected subtree is discovered and validated. No special
filename, central document list, manually typed digest or approval stamp is needed.

## Preview, then inspect the actual prepared candidate

An incomplete preview is useful while writing. Its missing-item report is generated
from code, not a checklist you maintain. The preview may show expected projections,
but it must be clearly distinguished from native observations from built artifacts.

Before release approval, use nonpublishing release preparation to build the real
candidate. Open its one review index: rendered guides/reference, consolidated installed
CLI help, served OpenAPI, supported Python prose, wheel metadata and image descriptions.
These native observations are extracted from the candidate, with complete raw evidence.
Inspect wording and behavior together, including executable journeys where applicable.

Approval uses the existing protected/offline release mechanism on this exact candidate.
Publication promotes those same bytes. A changed document, code revision, compiler,
policy, artifact or observed output means a new candidate, not a refreshed hash stamp.
Coverage answers "is anything missing?"; extraction/parity answers "did these words
reach these artifacts?"; human review answers "are they accurate and useful?".
Neither a compiler success nor a digest can answer that final question for you.

## Reference limitations

This prototype refuses binary assets/images until their explicit safety policy is
integrated. It does not build real release packages/images or provide the complete
native requirement extractor. The examples above are tested synthetic documents, not
an actual v1 corpus or evidence of human approval. Production must use the existing
renderer/publication machinery rather than publish this reference as another site.

## Reading documentation drift without more author bookkeeping

The plan/preview/check workflow produces one generated audit report. You do not add
any hashes, baseline identifiers, coverage dispositions or acknowledgements to front
matter. The trusted coordinator selects and identifies the comparison baseline.

In the prepared Contract Render, Documentation Audit shows what is missing, where
meaning changed without prose changes, which obligations changed, and what actually
reached installed help/OpenAPI/package surfaces. Open the affected subject to see its
old/new facts and selected Markdown section. The terminal gives a short view of the
same findings. A full report remains available when the summary is truncated.

FAIL means a mechanical blocker must be fixed. REVIEW means inspect the evidence and
wording; the existing wording may still be correct, so do not change it just to clear
a badge. PASS refers to the checks performed, not to human approval or prose truth.
A preview can have unverified native outputs. A first release has initial review, not
an invented prior release. Review the actual prepared candidate and approve its exact
bytes through the existing release process; there is still only one approval step.
