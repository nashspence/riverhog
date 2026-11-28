# What the finite experiments establish

## Source and measurement boundary

The comparison executes the actual independent reader and the actual `_stage_provenance` caller from main `be483fd6d567616d5fcd355202d61f1e682efe75`, alongside the proposed versions. The input callback is at the authenticated plaintext boundary. It counts actual source opens, returned bytes and stream finalization, descriptor iteration, SQLite rows, statements and VM instructions. The source finalization counter does not distinguish exhaustion from explicit close; separate maintained interruption/receiving tests cover those paths. The probe also inspects the actual query plans. It does not measure elapsed time.

The reader probe imports the maintained small fixtures from the proposed test module. The staging probe uses genuine neutral journals, real journal/corpus validators and checks exact staged bytes. Each reader observation exercises two complete API cycles against the same prepared view, followed by resource receiving. Journals, segments per journal, bindings and selected structure accesses vary independently. The selected-structure axis holds the archive fixed while varying the number selected.

There are **78 observations**: 44 reader observations and 34 actual staging observations, including the 512/513-binding page boundary. All completed on the final source/fixture/probe hashes recorded in [SOURCES.json](SOURCES.json). The source files remained unchanged during each probe. Compressed raw reports and their compressed/uncompressed hashes are listed in [evidence/counts.json](evidence/counts.json).

## Reproduced repeated work in the actual caller

This family uses two journal-segment descriptors per journal, one binding-page descriptor and one terminal descriptor: D = 2J + 2. Counts below are actual descriptor source opens, including terminal descriptors.

| Journals J | All descriptors D | Main: staging | Proposed: staging | Main: staging + complete bindings | Proposed: staging + complete bindings |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 | 20 | 4 | 32 | 4 |
| 2 | 6 | 42 | 6 | 60 | 6 |
| 4 | 10 | 110 | 10 | 140 | 10 |
| 8 | 18 | 342 | 18 | 396 | 18 |
| 16 | 34 | 1,190 | 34 | 1,292 | 34 |

Main's header-to-range subpath alone reads `2(J+1)D` descriptors. Actual staging also performs its initial scan, making `(2J+3)D`; a subsequent complete binding operation adds three D-wide descriptor passes, making `(2J+6)D`. These formulas describe this concrete caller and full-consumption fixture, not every possible archive access. The prepared caller authenticates D descriptors once and thereafter reads selected rows from its own projection.

At J=16 this is 35 times fewer descriptor source opens for staging. **It is not a claim of 35 times faster recovery.** Journal and binding payload access, decryption cost, semantic validation, filesystem work and later mapping still exist. At the plaintext boundary, removed descriptor opens indicate removed source work; only a matched encrypted run can establish the resulting elapsed-time improvement.

## The repeated scan was not merely moved behind a cache

For J = 1, 2, 4, 8, 16, the prepared header-plus-ranges phase returns 4, 8, 16, 32, 64 index rows. The corresponding observed SQLite VM counts are 53, 88, 158, 298, 578. Actual per-journal plans use `SEARCH ... (journal=?)`; the maintained actual-caller test also rejects a full scan for those queries.

Preparation has separate measured VM work: 358, 420, 544, 792, 1,288 in that family. SQLite B-tree insertion and seek costs still apply. These finite counts and source inspection establish removal of the observed J×D traversal, not a claim that every internal SQLite operation or whole recovery has strictly linear cost.

The binding boundary tests matter because an in-page increase alone would miss page growth. At 512 to 513 bindings, descriptors increase from 6 to 7; prepared descriptor opens increase from 6 to 7; binding payload opens increase from 2 to 4, preserving the existing verify-then-yield passes. Staging's journal index rows remain 8; the subsequent binding index rows increase from 2 to 4.

Selected structure reads remain fresh and content checked. On the fixed-archive selected-structure axis, selecting 1, 2, 4, 8 or 16 changes only the necessary structure source reads. This is an actual reader API observation, not a full `_stage_mapping` complexity qualification.

## How this fits the existing real-path evidence

The integration owner's [real-path diagnostic](https://github.com/nashspence/riverhog/issues/991#issuecomment-7231967781) observed independent recovery at 52.742 s and 87.147 s for its two F1/F2 cases. Its separate intrusive recovery trace attributed 32.857 s of a 38.020 s run to subprocess wait across 63 official age/batchpass invocations. These measurements motivate avoiding repeated decrypt/open work. They do not prove crypto CPU cost, a minimum unavoidable recovery time, a per-member slope or this proposal's speedup.

The same real-path diagnostic reported publication plus receiving at 99.387 s and 143.051 s. This recovery proposal does not address or explain that separate path. The [later sealed-payload finalization fixture](https://github.com/nashspence/riverhog/issues/991#issuecomment-7232111045) is a different experimental boundary and should remain separate. Do not combine the two into a checkpoint-first conclusion or subtract their times as though they were matched runs.

No per-file marginal budget is approved here. The maintainer's 1,000-file examples express the urgency of removing accidental work, but these experiments vary metadata dimensions, not 1,000 real archived members under a matched workload. Numerical contracts, comparability and available GitHub Actions wall-clock evidence remain with #1006 and the shared all#68 work; downstream whole-path/growth/recovery acceptance remains #1007.

## Reproduce without a new product command

Use a checkout with the proposed patch applied and the existing repository-pinned environment. Save the exact old reader and caller outside that checkout:

```bash
probe_baseline_dir=$(mktemp -d)
git show be483fd6d567616d5fcd355202d61f1e682efe75:some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py > "$probe_baseline_dir/_provenance_reader.py"
git show be483fd6d567616d5fcd355202d61f1e682efe75:some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/recovery.py > "$probe_baseline_dir/recovery.py"

python /path/to/reference/.reference/experiments/descriptor_probe.py \
  --repo "$PWD" \
  --baseline-file "$probe_baseline_dir/_provenance_reader.py" \
  --output /path/to/new/count-comparison.json --include-stage

python /path/to/reference/.reference/experiments/descriptor_probe.py \
  --repo "$PWD" \
  --baseline-file "$probe_baseline_dir/_provenance_reader.py" \
  --output /path/to/new/binding-boundary.json --include-stage \
  --axes bindings --sizes 512 513
```

`python` above means the locked workspace interpreter, not an arbitrary system interpreter. Output paths must be new. The probe is a finite reference artifact; the proposed maintained tests own the regression. It is not a new product profiling entry point, registry, CI gate or permanent benchmark package.
