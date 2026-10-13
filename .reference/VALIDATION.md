# Reference validation and limits

This revision is a non-authoritative integration handoff. The records below are
reference checks, not product qualification or completion of #968.

## Audited input

Source `e43df5b5acce5d730c28b48b084e87445fbaac3b`, tree
`9546274bcd5918db378d1d12311e7e9cf06aa309`, and prior reference
`4916504c9441a1dcde98d9904da6e0dfe9b729e7` were available in a full Git clone.
The fetched ZIP SHA-256 was
`12343c3e8c387f4a0d603019786ac7d347803bfd945550de3ecafffe85b1c235`;
its embedded manifest SHA-256 was
`2bad19fd9cb6792de97a4044276fc3caede54b01b2951d495e58905f74fbe24d`.
Both matched the clone service's identities. ZIP extraction did not preserve
executable modes automatically; tracked file modes were restored from Git's index
and the clean audited worktree verified before preparing reference additions.

## Performed on CPython 3.13.5 / Git 2.47.3

- Retained attribution suite: 22 unittest tests passed.
- Added oracle/sufficiency/two-way accounting suite: 21 unittest tests passed.
- All 29 focused committed-source byte/mode fingerprints matched.
- Reference checksum membership, Python syntax and local Markdown links checked.
- The two-file onboarding patch passed `git apply --check` and applied in an
  isolated temporary repository. Exact approved section, unchanged other sections,
  root links, patched test syntax and whole-guide integrity update were checked.
- Prepared guide: 2,021 words, within the unchanged 2,100-word guard. Its SHA-256 is
  `a42d88ba06ed5a74a8a2a3ce5ec66611af63281d630743189ed505312a016935`.
- Negative verifier probes rejected altered reference content and an added
  unmanifested file; a changed committed source seam required reconciliation.
- The initial staged whitespace check rejected blank unified-diff context lines in
  the patch file. Normalizing those blank context lines resolved it; patch
  application was rechecked. Staged whitespace and absence of source changes
  outside `.reference/` then passed.

The exact commands are in [HANDOFF.md](HANDOFF.md). Published tree, ordered parents,
branch lease/readback and comparison with audited main are recorded in #968 after
publication. These statements do not predeclare the outcome of remote publication.

## Not performed

No upstream project code was executed. No actual pytest collection, repository
unit suite, locked lint/type check, full Linux/image/install/native/provider work,
Contract Closure regeneration, browser reading, or hosted qualification ran here.
The focused guide test was parsed, not run against the upstream test environment.
No new binding schema or sample oracle was accepted as a product declaration.
Synthetic inputs assume reviewed requirements and trusted records; neither Python
example authenticates artifact custody or proves an expectation is independently
correct. The assertion-sensitivity example is not arbitrary mutation detection.

All source refresh outside `.reference/` is inherited unchanged from audited main;
only reference material is authored here. No production branch, release or provider
control is updated, and no CI success is inferred from publishing this handoff.
Integration must reproduce relevant cases through actual adapters, close the full
accounting boundary and obtain its normal final-source evidence.
