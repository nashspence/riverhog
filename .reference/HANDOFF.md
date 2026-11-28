# Independent recovery: prepare the selected descriptors once

Owner: [Riverhog #991](https://github.com/nashspence/riverhog/issues/991), answering [the integration agent's request](https://github.com/nashspence/riverhog/issues/991#issuecomment-7231803772). Convention: [#903](https://github.com/nashspence/riverhog/issues/903) and [Proffer](https://github.com/nashspence/proffer/tree/2ce5e15c3191fcdc2be70213c0170f57c852a883).

**First step:** reconcile the [patch](proposed/prepared-recovery.patch) with the current #1008 integration head, review the snapshot boundary in [DESIGN.md](DESIGN.md), then run the new small actual-caller tests. The patch applies to inspected #1008 head `382870b8ce3072c962f57a72cd9842984474002c` with byte-identical proposed outputs. No new package or dependency is needed.

## Recommendation

Use a **recovery-owned prepared metadata view**, with one private disposable SQLite descriptor index per recovery attempt. Reuse the independent reader's complete root/sequence validation and the server index's ownership pattern. Journal requests seek their own rows; they do not walk every descriptor again. Stream journal payloads into the existing staging files and export the already verified exact root bytes.

Keep payload/structure reads fresh and fixity checked. Preserve the separate current description/tag-head ciphertext fence before completion. Avoid a whole-archive plaintext cache, server imports, a new neutral package or another persistence framework.

## Concrete result

The proposal changes three internal recovery modules and adds one maintained test module. It also closes the actual journals/selection connections on failure and explicitly receives source iterators. The public recovery API, package dependencies, archive formats, official age/batchpass path and metadata-head fence are unchanged.

In actual `_stage_provenance`, 16 journals / 34 descriptors required **1,190 descriptor opens on main versus 34 with preparation**. Including a complete binding traversal gives **1,292 versus 34**. Exact staged bytes, real neutral journal/corpus validation and receiving passed. Indexed-query rows and VM observations show that the multiplier was not moved into SQLite. These are work-count results, not a measured recovery speedup.

- 33 new plaintext-boundary/caller/resource cases passed.
- 18 existing encrypted recovery cases passed with pinned official age 1.3.1 and batchpass.
- 15 existing reader/publication/ownership cases passed.
- Focused locked Ruff/mypy, patch integrity and exact applied-file comparisons passed.
- 78 finite old/new count observations passed on the final proposal bytes.

See [EVIDENCE.md](EVIDENCE.md), [VALIDATION.md](VALIDATION.md) and [evidence/counts.json](evidence/counts.json) for scope and identities. The encrypted tests use their existing low-cost fixture KDF; they are compatibility/correctness tests, not production timings.

## Consume only what is useful

| Next question | Route |
| --- | --- |
| Why this owner, and what does a prepared snapshot promise? | [DESIGN.md](DESIGN.md) |
| What changed, what proves it, and how does it fit #1008? | [INTEGRATION.md](INTEGRATION.md) |
| What work disappeared and what remains? | [EVIDENCE.md](EVIDENCE.md) |
| Exact proposed code and maintained regression tests | [proposed/prepared-recovery.patch](proposed/prepared-recovery.patch) |
| Reproduce the finite comparison | [experiments/descriptor_probe.py](experiments/descriptor_probe.py) |
| Actual checks, source identities and limitations | [VALIDATION.md](VALIDATION.md), [SOURCES.json](SOURCES.json), [proffer.json](proffer.json) |

## Source and authority

Audited/experimental base: `main@be483fd6d567616d5fcd355202d61f1e682efe75`. Later inspected #1008 contains only the already accepted guide/test amendment; product code is identical to that base. The reference branch changes only `.reference/`; proposed product edits are carried as a patch. This is a new finite slice, not a replacement for #1006's broader handoff and not adoption of #1001.

Produced/published by OpenAI ChatGPT, GPT-6 Astra Pro, with three same-model source/test reviewers. Requested by the Riverhog integration agent and explicitly commissioned by the maintainer in this conversation. Snapshot: 2026-11-27 America/Los_Angeles; source reads span November 28 UTC.

Evaluate and apply/adapt through the normal integration rail; **do not merge this reference branch**. Post a finite Proffer disposition in #991 against the actual published commit. Complete changed-source CI and a matched real recovery diagnostic remain integration work. #1007 retains downstream whole-path/growth/recovery acceptance. This slice creates no global hold and approves no numerical performance limit.
