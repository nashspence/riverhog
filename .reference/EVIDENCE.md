# Evidence, attribution and receiving

The current reading entrypoint is [DESIGN.md](DESIGN.md). This file separates actual static verification from historical observations and receiving-owner reports.

## Source identity

| Identity | Exact value |
| --- | --- |
| Audited source and sole reference parent | `d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e` |
| Audited base tree | `98cf298e60e4ba9c178229eae907e164265f6e48` |
| Requested source ref | `codex/964-session-owner` / PR #979 |
| Historical phase reference | `8b639901c4dbe157dce904acec718a23f580c6ca` |
| Historical execution source | `3b47cbba770c31eb1487b50f7e3b16533c10e827` |
| Prior Core-stepping reference | `46ace062078d08cd70f508a3f3a7d834a92cbb40` |
| Prior occupancy reference | `bcb8de725c1958e685fbc4afc417f32bedaece13` |

Product files in this reference are unchanged from the audited source. The only full-tree changes from historical execution source 3b47 to d4d7 are `_session_checkpoint.py` and its maintained checkpoint test file. The actual diff, both versions' file hashes and unchanged phase-owner sources are reconciled in [source-audit.json](evidence/source-audit.json), [protocol-audit.json](evidence/protocol-audit.json) and [growth-inventory.json](evidence/growth-inventory.json). No runtime measurements were refreshed by changing the reference's parent.

## Historical bytes actually read

The exact historical raw is [run.json.gz at 8b639901](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/observation/run.json.gz). Its decompressed bytes equal the available local `native-phase-observation-r2/run.json`:

- **Bytes:** 3,240,236.
- **SHA-256:** `019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b`.
- **Retained histories:** initial 107, checkpoint 125, correction 247 advances; total 479.
- **Finish prepares:** 25, 26 and 50, respectively; total 101.
- **Selected final-child/promotion pairs:** raw global zero-based ordinals 100/101, 207/208, 225/226, 332/333 and 472/473.

The [offline extractor](evidence/reconcile_inventory.py) reads explicit historical trace and source inputs using the Python standard library. Its [inventory receipt](evidence/growth-inventory.json) retains the selected controls, actual call counts, source and input pins, and analysis method. This is finite offline reanalysis of an existing trace. It does not invoke Riverhog, create a fixture, re-execute a phase or establish behavior of a hypothetical combined transition. [INVENTORY.md](INVENTORY.md) explains count scopes and growth composition.

## New diagnostic figures: attributed, not reproduced

The exact [request body](evidence/request.md) is retained with the relevant [context comments](evidence/context.json). All four bodies and URLs were compared with the live #991 comments before publication: 6887181395, 6887528297, 6889902338 and 6890881026. The request was still the latest issue comment at that check.

The independent source audit also cites earlier qualification comment 6889181049. Its [selected body](evidence/qualification-comment.json) was separately compared with the live issue comment. The prior local container remains identified in the audit as provenance; this packet retains only that additional selected comment.

| Supplied artifact | Reported SHA-256 | Status in this handoff |
| --- | --- | --- |
| Occupancy reference manifest | `8a1262afb85bd1fa831b644b86605ddc4032be6a61b2ce85b988bf65074bec3c` | Identity of the prior published packet, distinct from this packet's manifest. |
| Complete function-CPU diagnostic raw | `dd39bd52cbe6d8e5b5e3b56c51a5f24e01fb7c4d79e8e8f72c50759fb74a743c` | Digest supplied in 6890881026; raw bytes not supplied or independently verified here. |
| Reduced-observer partial diagnostic raw | `bd3dae6a81b299627c0f2fde341639d760affbb1d69a47a92c0dce18bd777dd2` | Same attribution; 440/479 advances remains partial. |
| Offline function/caller accounting | `0e7db5c818230641cc33c10d7629b4e03d33e91592b7ff8885f3bb7a5bab41f1` | Same attribution; no claim to have rerun that accounting. |

The complete diagnostic's 23.491-second helper time, 18.949-second wall / 10.367-second thread-CPU retained-stage totals, and 482 commit API calls at 8.713-second wall / 0.132-second CPU are receiving-owner observations. They do not identify an avoidable-commit count or separate fsync, lock or driver causes. The reduced-observer run's two completed cases, 2.741 seconds exclusive thread CPU, 5,562 encodings and 405 compressions preserve their stated boundaries; inclusive ancestors are not additive. Per-execution child identities were reported bijectively normalized, with dependent checkpoint hashes excluded from direct cross-run comparison. None of these observations is presented as a speedup comparison or all-growth clearance.

d4d7's full CI qualification is also the receiving owner's report. No CI suite or product test was executed for this reference-only response. The retained one-step/per-step-adoption decision remains controlling.

## Published packet map

| Published artifact | Purpose / exact analysis source |
| --- | --- |
| [HANDOFF.md](HANDOFF.md) | Short #903 provenance, scope, source relationship and first receiving step. |
| [DESIGN.md](DESIGN.md) | Current synthesized answer: one candidate, direct counterexample, Core C1 distinction and exact contract gap. |
| [INVENTORY.md](INVENTORY.md) | Historical control/commit inventory and aggregate growth analysis; copied from `native-granularity-inventory.md`. |
| [PROTOCOL.md](PROTOCOL.md) | Transactions, currentness, capacity/error, restart and original receiving; copied from `native-granularity-protocol.md`. |
| [VALIDATION.md](VALIDATION.md) | Independent source/test review and static witness limitations; copied from `native-granularity-review.md`. |
| [growth-inventory.json](evidence/growth-inventory.json) | Inventory/source receipt; copied from `native-granularity-growth.json`. |
| [protocol-audit.json](evidence/protocol-audit.json) | Protocol source/context pins; copied from `native-granularity-protocol.json`. |
| [source-audit.json](evidence/source-audit.json) | Independent exact-source audit; copied from `native-granularity-source-audit.json`. |
| [reconcile_inventory.py](evidence/reconcile_inventory.py) | Retained stdlib-only historical inventory extraction; copied from `native-granularity-reconcile.py`. |
| [context.json](evidence/context.json), [request.md](evidence/request.md) | Exact supplied GitHub comment bodies and focused request. |
| [qualification-comment.json](evidence/qualification-comment.json) | Exact selected earlier qualification report, retained as an attributed comment. |
| [extraction-first.json](evidence/extraction-first.json), [extraction-first-error.json](evidence/extraction-first-error.json) | Byte-exact first offline extractor in a UTF-8 JSON envelope, including its original whitespace, and its schema-error receipt; no product work ran. |
| [packet-verification.json](evidence/packet-verification.json) | Actual finite static verification receipt, with counts, timing, source/tree checks and file snapshot. |
| [MANIFEST.json](MANIFEST.json) | Exact SHA-256 and byte count of every packet file except the manifest itself. |

Analysis JSON may preserve original absolute workspace paths and analysis filenames as provenance. The table maps those analysis deliverables to their byte-identical published locations. Such paths are not a dependency on another conversation or an assertion that unpublished diagnostic bytes were available.

## Actual verification and limits

Static verification checks exact source blobs against d4d7, explicit line ranges, the two-file historical delta, supplied context bodies, relocated report identity, report hashes, JSON, pinned source links and local artifact links. It verifies that all paths outside `.reference/` equal the base and that no untracked product file was introduced. The retained inventory extractor is independently reconciled against the selected raw controls and historical published bytes. Actual check counts and elapsed time are recorded in [packet-verification.json](evidence/packet-verification.json), rather than inferred from the existence of a script or declaration.

The first offline extraction stopped before emitting a summary because `EntryPreparation.prepare` has a null `after_state` in this trace. The corrected extractor uses the actual paired `_apply.state` and checks equality wherever a preparation's after-state exists. Its finite extraction completed successfully; offline refreshes updated the report hash after wording/path clarification and the extractor hash after removing one trailing space for clean diff verification. These were JSON/source-evidence operations, not runtime retries. The original error and script are retained above.

Publication uses a new commit with d4d7 as its sole parent and the verified local tree. The published branch is fetched and its actual commit, parent, tree, product equality and every manifest entry are checked before the owning-issue handoff. The owning comment records the actual published SHA and manifest digest; no pre-publication commit identity is substituted. Prior reference histories are preserved.

No product or test imports, fixture creation, new fault injection, tests, CI, product turns, runtime profiling, full caller census, archive publication/completion run, benchmark or member-scale campaign was performed. The counterexample is a source-derived execution-path distinction; inspected maintained tests are not reported as executed qualification. No runtime illustration was necessary to establish the missing validation boundary, so no cooperative workload deadline or receiving-envelope experiment was started or retried.

No implementation, commit removal, weakened durability, acceptance cache, changed concurrency or provider work is supplied. This packet is not product acceptance, a new release gate or authority to merge. Whole-path G10/G12/G13 and held campaigns retain their existing disposition.
