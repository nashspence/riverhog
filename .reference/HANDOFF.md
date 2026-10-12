# #963 — validation evidence and advisory performance

External reference under #903. The [#963 decision 6125400031](https://github.com/nashspence/riverhog/issues/963#issuecomment-6125400031) owns the maintainer's
corrected ontology; the former timing-approval policy is superseded.

Branch: reference/external/963-validation-timing
Direct parent: 443adc3b45661401e9a87dad3641e1888e98ddbd
Source patch base: main@152d095038df0c0d0ed642cf265d47b11c65a754
Source tree: 991e994457b487a7dca016b7fc0409485cef25c6
Producer: OpenAI ChatGPT / GPT-6 Astra Pro; effort/configuration not exposed.
Requester: Riverhog maintainer. Publisher: connected GitHub integration.

## Read this distinction first

**A performance target has no authority to block anything.** It is optional,
advisory metadata. A miss, missing target, unavailable comparison or serious
performance concern is not a failed test, unmet contract, required investigation,
handoff blocker, or condition requiring permission to proceed. Significant
findings support evidence-backed recommendations of appropriate strength.

Correctness, independently owned growth/resource/safety contracts, and valid
required measurement/evidence remain enforceable. If slowness violates a genuine
safety or resource contract, that independent contract owns the failure; the
profiling goal does not. Do not evade this distinction by renaming a seconds goal
an acceptance criterion, evidence-completeness rule or review requirement.

## One source patch, not the old patch chain

Start with [GROWTH_RESULTS.md](GROWTH_RESULTS.md). Apply ONLY
[validation-evidence.patch](validation-evidence.patch) to a separate checkout of
the exact source base above. It supersedes both former runtime patches and their
approval-state tests. Those files were removed from this tip, not deleted from
Git history. Do not apply an old foundation first and then try to fix its ontology
with a prose overlay. Do not merge this external branch.

The patch separates required work/conditions/growth bindings from optional
advisory targets; absence needs no approval. It preserves command failures and
required growth results through runner and aggregate exit statuses, while target
outcomes cannot affect them. An altered, added or removed goal does not change
the identity of required work or retroactively invalidate truthful evidence.
The recorded target is used only to reconstruct a supplied advisory observation.

[AGENTS.after-onboarding.patch](AGENTS.after-onboarding.patch) applies to the
current approved #961 two-root-document guide, not main's older guide. It uses
the maintainer's exact corrected sections (formatting and existing link targets
restored) and handoff sentence. README and all unrelated guide text are unchanged.
Input SHA-256: 1c427d38b88c9a05fe105dea5bcc862efec52ff516233110fa0ef88b43692f44
Output SHA-256: d2cf10da13a9b4976911b5abd101c5ecfe609b1598a5c9c1bb57232160e7302e
Do not restore docs/ or an abandoned pre-v1 schema. Keep Alembic/StateSchema and
supported future-upgrade tests. This publication does not modify main's guide.

## Focused integration order

1. Consume current #961; reconcile the exact source/guide patches, not old hashes.
2. Integrate one known check: real work -> automatic elapsed/outcome evidence ->
   independent growth/evidence validation -> aggregate result -> advisory report.
3. Run both positive and negative ontology tests. Then complete maintained entrypoint,
   CI/aggregate/phase coverage and owner-backed witness artifact verification.
4. Supply the tested foundation and exact source to #964. #964 owns product growth
   declarations and witnesses, not another shared timing-policy authority.

Use make profile for maintained profiling scenarios; helper scripts are internal
adapters, not new developer-facing profiling commands. Keep actual rates and
matching measured references; a speed goal is not a measured baseline.

See [VALIDATION.md](VALIDATION.md) for exact reproduction and limitations. The
reference is not full #963 integration, dynamic growth coverage or qualification.
No performance-target attainment, target selection or speed disposition can become
a prerequisite to finishing this issue or handing the foundation to #964.
