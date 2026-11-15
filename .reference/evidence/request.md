Received [Native phase handoff 6887181395](https://github.com/nashspence/riverhog/issues/991#issuecomment-6887181395), reference **8b639901c4dbe157dce904acec718a23f580c6ca**. All 33 manifest entries, sole-parent/evidence-only relationship to fully qualified 3b47cbba7, current helper/reader pins and historical runtime equality verified. The lossless raw hash verified; receiving-owner offline reconciliation matched every guard and all 479 advances without product execution.

The tiny histories reconcile to 107 / 125 / 247 advances. Separate admission, cleanup, completion and publication phases explain much of the count. Repeated budget-start work was about 1% of measured advance CPU; checkpoint controls, database calls and explicit commit scopes were larger costs. Correction revalidated all six effective assertions after one replacement; that aggregate-work concern remains distinct. Zero graph adjacency visits do not qualify nontrivial DAG growth. The historical 418 raised Core catalog attempts and actual ready-key/worker occupancy remain unclassified/unmeasured. Retained slots are not a thread executor.

### Received codec refinement and integration

One bounded codec refinement on clean 3b47cbba7 completed 479 retained advances with exact ordinary oracles, all original resources received, 77 hooks restored and scratch removed. Its whole maintained lane was 7.423 s. This differently instrumented/conditioned observation is not compared with the producer's 13.400 s or product throughput.

It recorded 5,185 prefix decodes, including two same-length/same-hash inputs within each of 945 load, 479 fence and 479 publication observations. The 1,424 load/fence pairs identified repeated decoding within one fresh observation; three initialization repeats are additional.

Pushed **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e** in PR #979. The private prefix reader now returns its exact fresh row plus the parsed state used to check it. Initialization/read/fence consume that state within the same invocation. Publication still independently parses its selected prefix. Every fresh SQL read, source/worker/algorithm/prefix/predecessor predicate, canonical codec, transaction and compare-and-set remains; no cross-call decoded state or acceptance cache was added.

Pinned lint and **107 focused Native owner cases** passed, including nine new warm-corruption cases and **nine growth witnesses / nine executions / 130 samples**. Stale/corrupt state, rollback, durable process interruption/restart, cold/wide checkpoints and retained Session/EntryReplay behavior were included. Original worker/make exits and cleanup received.

A second finite diagnostic used the exact archived initial/checkpoint/correction bytes. All 479 semantic progress/control traces, noncodec method counts, DBAPI attempts, source deliveries and materialized outcomes matched. Prefix decodes fell **5,185 → 3,758**; all 1,903 ValidationCheckpoint parses remained. This is work-count evidence, not a throughput comparison; diagnostic construction and hook costs prevent comparing whole-lane timings.

Current [CI 43664490758](https://github.com/nashspence/riverhog/actions/runs/43664490758), tested merge **05abffaef7f87a93b4949fcd6981e4ce6d033864**, is running for 7,579 selected supplemental cases, all 26 workloads and **48 growth witnesses / 54 executions**. All previous case IDs remain; nine cases are new source tests and 69 existing owner cases expand supplemental receiving. Predecessor 3b47cbba7 remains fully qualified.

### Focused design/reference request: bounded Core stepping

Audited receiving checkpoint is publicly available: **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**. Reconcile its narrow prefix-decoding change first; full CI is pending.

Investigate whether one explicitly bounded Core journal-owner unit can drive several existing Native advances, preserving Native phases, per-phase durable commits and fresh per-step authority checks, then adopt catalog progress at the correct boundary. The purpose is to reduce repeated outer stamps/prerequisite/control coordination. This is a design question, not a prescribed patch or permission to loop under unchanged accounting.

Please provide a non-authoritative source-owned design/reference that:
- Defines explicit per-unit step/work/resource admission and every actual child multiplicity. Preserve bounded coordination, fairness, independent growth obligations and total/inclusive composition; no hidden extra work or archive-size cap.
- Explains Native progress committed before catalog adoption, stale source/worker/certificate replacement, capacity pauses, partial failure, cancellation, reply loss, durable restart, other ready journals, retained readers and original receiving. Identify checks that remain fresh at each inner step and final adoption.
- Keeps the existing Native state machine and durable transactions as the initial scope. Phase fusion, removed commits or changed concurrency require a separately justified design/authority discussion.
- Evaluates growth first, including repeated whole-resource preparation and distinct correction-wide work. Do not label all remaining cost constant or claim G10/G12/G13 closure.
- Uses static/retained evidence first and only a few tiny actual Core/Native turns if needed, under an approximately two-minute finite cap. Preserve exact results, every child/primitive observation, original exits and cleanup. No publication campaign, full census, broad suite or 1,000+ run.
- Recommends leaving the change out if invariant/resource/error-order proof does not support it, and identifies any required owning design decision.

Native transaction cost, catalog-raise classification, transport/lifespan cost and parallelism occupancy remain tracked here. Whole-path #964 acceptance remains open. Full census repetition and aggressive 1,000/10,000/100,000 campaigns stay held; no timing gate, merge, release or provider activation is added.
