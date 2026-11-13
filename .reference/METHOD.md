# Measurement method for the #991 reference

Audited source: `8ce49de94c32ae39562aab5ea4178b1dc8c75e77` on PR #979. This is research input for the integration owner; no product edit, new acceptance gate, or large run was performed by this researcher. Source findings below were checked against that checkout. The current #991 comments already contain serial one-, two-, and four-member diagnostic results. Repeating that timing ladder without a new question would add cost without closing the missing attribution/composition evidence.

## Method used for the delivered diagnostic

The reference has now performed **one serial, isolated one-member maintained publication** with coarse phase clocks and a small fixed set of primitive counters; see [RESULTS.md](RESULTS.md). Consume that result and the newer integration checkpoint first. Reproduce only to resolve a concrete question. The next integration action is [complete-journal caller accounting](NEXT_SLICE.md). The method for the delivered initial diagnostic follows. Its question is: *how many catalog, admitted Native SQLite, checkpoint, and prefix operations occur, on which actual thread/phase, and how much measured work versus residual remains?* It is an attribution experiment, not a throughput comparison. Maintain the ordinary archive contents, authentication, finalization, exhaustive fresh/warm/evicted verification, and original helper/workspace receiving.

Use `make profile args="--scenario collection-publication ..."` through the existing `args=` convention; the actual command owner is [scripts/transfer_profile.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/scripts/transfer_profile.py) `_profile_collection_publication`, which creates an isolated helper and receives it through [scripts/validation_process.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/scripts/validation_process.py) `run_validation_child`. Keep first disposable instrumentation supplied through `.reference/`, with its bytes/digest and installation boundary in the receipt. Do not turn that reference harness into a parallel developer-facing product command. Accepted maintained instrumentation belongs in the existing profile owner and its owner-local tests.

A reference-only `sitecustomize` arrangement must activate only inside the intended publication helper, before the selected owner functions are called. Do not accidentally instrument `make`, uv, the outer profile driver, or unrelated subprocesses. Record coverage explicitly and fail the diagnostic if expected hooks were absent. A whole-process observer added through startup changes the execution environment even when product bytes stay identical: both identities belong in the receipt.

## Clocks and accounting rules

Record monotonic elapsed deltas with `time.perf_counter_ns()`, helper-wide CPU with `time.process_time_ns()`, and local operation CPU with `time.thread_time_ns()` in the same thread before and after the operation. Record clock implementation/resolution. Python documents that the elapsed clock includes sleep, process CPU is process-wide, and thread CPU is thread-specific [P1]. A process CPU value captured around concurrent operation A includes other threads' work and cannot be assigned to A.

Keep four different quantities:

1. **Elapsed critical-path interval:** start/end of the profile or contiguous lifecycle phase.
2. **Cumulative work:** calls, bytes, rows, frames, objects, attempts and admitted operations, across all owners.
3. **CPU:** exclusive per-thread operation intervals, helper process total, and unexplained CPU residual.
4. **Wait/availability evidence:** measured admission outcomes and time spans, ready-work backlog, lock waits where independently observed, scheduling/IO gaps, and unknown wait.

`wall - thread_cpu` is an off-CPU opportunity interval, not a measurement of a particular lock or I/O wait. It can include scheduler delay, GIL competition, sleeping, work delegated to another thread, and uninstrumented calls. Negative or inconsistent subtraction is a diagnostic defect or clock-boundary mismatch, not something to clamp to zero invisibly.

For synchronous nesting on one thread, an operation's exclusive time is its inclusive time minus the union of its direct child intervals. Child CPU similarly belongs to its caller once. For concurrent work, sum CPU/work across distinct execution owners, but use interval unions for elapsed coverage. Do not sum the parent's elapsed time, child's elapsed time, and worker duration. A helper's `process_time` must not be added again to the contained worker thread totals.

Use a finite event vocabulary and retain the important spans, not an unbounded stream of per-SQL rows or stack strings. A span record needs `run/source/observer`, phase, owner operation, caller operation, invocation/attempt identity, process/thread identity, start/end clocks, input/output dimensions, result, and classification confidence. Aggregate identical leaf work by finite `(phase, owner, primitive, thread-role)` keys. When thread IDs can be reused, pair them with an observed lifetime/generation; never infer continuing identity merely from a repeated numeric ID. The thread's own operation wrapper can measure CPU safely. Do not enumerate threads then call `pthread_getcpuclockid` on a stale identifier: Python explicitly warns that expired IDs can have undefined behavior [P1], [P3].

The end-to-end phase wall totals should reconcile to the measured helper interval with a named residual. Record observer-installation and helper-startup boundaries explicitly: a phase clock installed after imports cannot account for earlier startup or the outer parent process. The parent profile boundary remains a separate interval. CPU totals should reconcile to helper CPU using exclusive same-thread work only; any startup, library/native, observer, or uninstrumented work remains an explicit residual. A large residual is a useful result and determines the next focused hook. It must not be redistributed in proportion to samples.

## Concrete hook map and its limits

| Boundary | Existing source hook | What to record | What this does not establish |
| --- | --- | --- | --- |
| Outer lifecycle | [scripts/transfer_profile.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/scripts/transfer_profile.py) `_profile_collection_publication` | Parent elapsed, helper identity/exit, run/workload/source, received cleanup; original failure outcome | Parent process CPU is not child CPU; successful child signal alone does not prove cleanup |
| Fixture phases | `canonical_history_scale.publish_shared_history` progress callback | Setup-to-first-append; each append if owner wrapper is installed; primary-history retrieval; supplemental/shared preparation and staging; finalization; post-finalization assertions/removal | Existing progress notifications are sparse at powers of two and do not provide every invocation or full phase subdivision |
| Full verification | `publication_profile.run_publication_profile`, `publication_evidence.measure_published_history`, `verify_stored_publication` | Each reader state, archive objects/reads/bytes, each major check category, exact verified result | Qualification observer work is not production publication cost; warm Native cache is not cleared/warm OS storage qualification |
| Catalog SQL | SQLAlchemy engine `before_cursor_execute` / `after_cursor_execute`, with `handle_error` cleanup | Count, elapsed/CPU cursor interval, finite statement family, rows fetched separately, commit/rollback separately, errors | Cursor execution excludes some result materialization and transaction finalization; direct Native SQLite bypasses SQLAlchemy |
| Admitted Native SQL | `sqlite_admission._Cursor.execute/executemany/executescript`, `fetchone/fetchmany/fetchall/__next__`; `_Connection.commit/rollback` | Actual admitted calls and fetched rows, synchronous elapsed/CPU, failure count, execution thread | One `executemany` call can contain many statements; bounded returned rows do not bound engine row visits; connection shortcuts delegate to cursor methods and must not be counted twice |
| Validation ledger | `_session_checkpoint.ValidationLedger.load`, `_fence`, `_publish`, `_prefix` | Counts and parent/caller relationships, checkpoint/prefix dimensions, inclusive/exclusive cost | Fences are required authority checks; repeated calls do not alone prove removable work |
| Prefix publication | `_journal_prefix.JournalPrefix.prepare`, `prepare_publication`, `_apply`, `commit` | Entry/frame/byte multiplicity, actual per-stage primitive work | Owner-local linear work can be multiplied by a repeated outer traversal |
| Physical work/admission | `services/physical_execution.PhysicalExecutionOwner._try_submit`, `collect`, actual work callable boundary | Submission outcomes, accepted starts/completions/receiving, slots occupied, terminal/forward lane, ready-to-admit interval, dependency | This owner explicitly has no extra internal queue beyond retained slots; full slots include work awaiting original receiving, not just active CPU |

SQLAlchemy documents before/after cursor event timing and distinguishes SQL execution, fetch, and ORM result processing [P4]. Match its failure/nesting behavior, but avoid its demonstration's raw SQL/parameter logging. Use a connection/context/thread stack, immutable statement categories, and literal-free aggregate counts. `executemany` row counts require observing consumed parameters without pre-consuming generators or changing lifetimes.

The admitted Native observer does not see all Native SQLite. Disposable `JournalStore` and `MemberHistoryClosure` paths default to ordinary `sqlite3.connect`, and the exhaustive observer uses those paths. Their work remains in broader owner spans or the unexplained residual; never label admitted-cursor totals as all Native SQL. Preserve the actual connection/factory, rather than replacing it just to obtain a prettier count.

The native observer must preserve the original admitted connection, cursor, reservation and exceptions. [tests/support/sqlite_observation.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/support/sqlite_observation.py) already supplies an observation pattern that preserves admission; reuse its principle rather than replacing the SQLite factory with unrestricted connections. The `_Connection.execute` convenience method calls `_shortcut`, which calls `_Cursor.execute`: choose a leaf count or mark the two levels as parent/child. `fetchall` may be counted using the returned list length; never issue an additional query to count rows while timing the operation. Observe `StopIteration` as normal iterator completion, not a database error.

Do not replace global `Thread.run`, `threading.setprofile`, or `sys.setprofile` to pursue this slice. The prior thread-entry experiment stalled. If deterministic call profiling is later needed, profile one owner-controlled synchronous scope and keep that run separate from throughput measurements. Python's profiler documentation explicitly distinguishes profiling from benchmarking and defines exclusive versus cumulative time [P2].

Passive stack samples show where a thread was observed, not how much CPU it used. Python's `_current_frames()` can show sleeping or blocked threads, and its snapshots can already be stale when examined [P5]. Keep sample interval, actual sample count, delay/missed samples, thread coverage, and sampler CPU. Never convert the fraction of heartbeat/poll stacks into a CPU percentage. Native code and short-lived threads can be poorly resolved. On Linux, optional `/proc` task CPU observations need PID/TID/start identity and missing-thread accounting; aggregate CPU captured by the owner remains the receiving-boundary reference [P6].

## SQLite follow-through only after attribution

An execution count is not a proof of physical engine work. If cursor work materially contributes, inspect representative query plans on the exact schema/statistics and use the existing owner-local observer tests. SQLite's `EXPLAIN QUERY PLAN` describes scan/index strategy, but its textual output is not a stable application interface [P7]. `sqlite3_stmt_status` counters can expose full-scan steps, sorts, automatic indexes and VM work when the actual owning adapter makes them available [P8]. Standard-library Python does not expose that C interface as a direct public method: do not promise exact VM counts from a mere SQL trace.

Python's SQLite progress handler is another possible diagnostic, but it installs a connection callback at VM-instruction intervals and can abort work on nonzero return [P9]. Do not replace an existing resource/cancellation callback, poll SQL from the callback, or silently claim exact instruction totals. A connection's `set_trace_callback` alone reveals executed statements, not row/VM complexity, and expanded SQL can expose values. Prefer bounded source-owned counters first.

Transaction costs need separate accounting: catalog versus Native connection setup, journal mode, selected synchronous policy, commit count, checkpoint work and live-reader effects. SQLite WAL permits one writer at a time; automatic checkpoints can make some commits expensive [P10]. This is a mechanism to investigate, not evidence that fsync/checkpointing explains this repository's measured cost. Preserve selected durability and custody semantics. Existing disposable Native stores can use `synchronous=OFF`, while retained stores use their stronger policy; do not change a selected policy, bypass a lock or add an opportunistic cache merely to improve timing.

## Observer integrity and overhead

Before the real diagnostic, use a bounded synthetic smoke check to verify wrapped return identity, unchanged exceptions including `BaseException`, thread-local nesting, counters on successful/error/iterator paths, no parameter logging, and cleanup restoration. It can estimate counter/timer overhead per call but cannot produce a corrected production time. Report the smoke inputs and total observer calls.

Use aggregate counters in memory and write outside hot scopes. Preserve original exception propagation and original owner receiving; instrumentation failure invalidates the diagnostic instead of turning a failed product operation into success. On the final receipt include observer version/digest, instrumentation groups, clock costs, recorded calls, buffer size/overflow state, sampler CPU if any, coverage and limitations. Treat dropped/truncated evidence explicitly; a cap on diagnostic records is not a limit on archive work.

A later matched uninstrumented/low-overhead run is useful only after a concrete change merits throughput comparison. Use the same source/workload/resource conditions and serial isolated catalogs, preserve every attempt, and report ranges or repeated paired observations rather than subtracting a guessed constant observer tax. Initial attribution timings must remain marked instrumented. Statistical tools cannot rescue unmatched sources, history geometry, progress mode or concurrent load.

## Progressive decisions without a new timing gate

1. **Explain one actual run.** Identify high-cost categories across every lifecycle phase, their owners, purpose, multiplicity, CPU/off-CPU contribution and residual. Assign `required`, `justified repetition`, `potentially avoidable`, or `unexplained`; use exact/observed/estimated/source-inferred labels.
2. **Check composition before optimizing.** Map each costly caller to actual delegates and source-owned growth declarations/tests. Existing cold checkpoint tests and prefix preparation tests prove their declared owner-local metrics; the parent loop's multiplicity remains a separate obligation. Count repeated entry/replay work, setup work, total metadata bytes, seeks/rows, failed attempts and retained-state work.
3. **Choose one discriminating small contrast.** Use existing 1/2/4 evidence where it already answers the question. A new primitive-count contrast can change member count with fixed bytes/member, then fixed total bytes; independent fixed-member payload/history/state contrasts separate mechanisms. Do not let `F=N`, payload bytes and physical journal count covary in every case and then call the result a complete member bound.
4. **Use owner-local boundary cases.** Exercise page/step/cache edges, zero results, cold/evicted state, failed/interrupted/retry/receiving paths with the maintained implementation. These are cheap opportunities to expose repeated traversal without a thousand-member end-to-end run.
5. **Correct demonstrated unnecessary work within current authority.** Add a maintained regression that observes the actual repeated work and verifies exact output/lifecycle behavior. Compare before/after counts on identical inputs. Request design input only for a concrete change to an owning semantic/security/custody contract.
6. **Profile concurrency deliberately.** Separate no-ready-work dependencies, one-scope ordering, admission refusal, registry contention, actual SQL writer waits, expensive receiving and implementation serialization. Under `PhysicalExecutionOwner`, a retained slot can be completed-but-unreceived. More workers may therefore worsen contention or retained state. Show ready independent work and a critical-path opportunity before testing a bounded concurrency change; retain current budgets/authority and original receiving.
7. **Record a finite disposition in #991.** State what was explained/corrected, what growth evidence remains and what substantial cost remains unresolved. Current maintainer priority holds costly campaigns while these concerns remain. Once the owner records a concrete disposition, resume the previously authorized larger rail selectively. 1,000 success does not establish 10,000/100,000 conformance; no one-sample extrapolation or automatic escalation is justified. Timing remains advisory; correctness/growth/evidence integrity retain their existing force.

For factor contrasts, record independent dimensions: member count N; total payload B and largest member; shared-root fanout F; documentary member extent C; retained physical journals K and required subset Q; retained history entries/bytes/dependency edges; catalog/prepared-state population; retry/restart state; explicit cache state; active/ready/retained execution population. Keep the factor changed and its resulting metadata dimensions visible. NIST's experimental-design guidance supports choosing objectives, variables and designs deliberately; it does not prescribe a full factorial campaign for this cost-constrained investigation [P11].

## Maintained validation landing points

If instrumentation is accepted, put receipt/argument/execution integration in the existing [scripts/transfer_profile.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/scripts/transfer_profile.py), [tests/support/qualification/publication_profile.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/support/qualification/publication_profile.py) and their tests ([tests/unit/test_publication_profile.py](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/tests/unit/test_publication_profile.py), `test_transfer_profile_script.py`). Add assertions for source/observer identity, missing/invalid instrumentation, failure receipt, exact child receiving, and untouched workload/verification. Place a proven growth correction's regression with its actual owner, e.g. `test_validation_checkpoint_growth.py`, `test_preparation_growth.py`, or catalog/service owner tests; do not build a parallel `.reference` compliance registry. The reference packet is an integration aid, not proof that maintained composition is complete. The seven decisions above are a routing method, not seven new compulsory campaigns: consume the actual receipt and choose the next smallest useful owner slice.

## Primary sources checked

All accessed 2026-11-13. Python references describe the 3.12 series; this checkout pins 3.12.3. SQLAlchemy 2.0 documentation is a later patch in that series; validate hooks against the pinned installed 2.0.51. These references support method/mechanism, not measured Riverhog diagnoses.

- **P1** Python 3.12, `time`: https://docs.python.org/3.12/library/time.html.
- **P2** Python 3.12 profiling: https://docs.python.org/3.12/library/profile.html.
- **P3** Python 3.12 threading: https://docs.python.org/3.12/library/threading.html.
- **P4** SQLAlchemy 2.0 performance FAQ: https://docs.sqlalchemy.org/en/20/faq/performance.html.
- **P5** Python 3.12 `sys._current_frames`: https://docs.python.org/3.12/library/sys.html#sys._current_frames.
- **P6** Linux kernel `/proc`: https://docs.kernel.org/filesystems/proc.html.
- **P7** SQLite `EXPLAIN QUERY PLAN`: https://www.sqlite.org/eqp.html.
- **P8** SQLite statement status: https://www.sqlite.org/c3ref/stmt_status.html.
- **P9** Python 3.12 sqlite3 callbacks: https://docs.python.org/3.12/library/sqlite3.html#sqlite3.Connection.set_progress_handler.
- **P10** SQLite WAL: https://www.sqlite.org/wal.html.
- **P11** NIST/SEMATECH experimental design: https://www.itl.nist.gov/div898/handbook/pri/section3/pri3.htm.

[P1]: https://docs.python.org/3.12/library/time.html
[P2]: https://docs.python.org/3.12/library/profile.html
[P3]: https://docs.python.org/3.12/library/threading.html
[P4]: https://docs.sqlalchemy.org/en/20/faq/performance.html
[P5]: https://docs.python.org/3.12/library/sys.html#sys._current_frames
[P6]: https://docs.kernel.org/filesystems/proc.html
[P7]: https://www.sqlite.org/eqp.html
[P8]: https://www.sqlite.org/c3ref/stmt_status.html
[P9]: https://docs.python.org/3.12/library/sqlite3.html#sqlite3.Connection.set_progress_handler
[P10]: https://www.sqlite.org/wal.html
[P11]: https://www.itl.nist.gov/div898/handbook/pri/section3/pri3.htm
