# Wake publication maintenance after accepted readiness

Owner: [Riverhog #991](https://github.com/nashspence/riverhog/issues/991), responding to [the focused activation request](https://github.com/nashspence/riverhog/issues/991#issuecomment-7233398358). Convention: [#903](https://github.com/nashspence/riverhog/issues/903), with a [Proffer handoff record](proffer.json).

**First step:** review the post-commit notification boundary and [six-file patch](proposed/idle-activation.patch), reconcile it with the current integration source, then run the focused activation/readiness/wake tests. The patch was actually applied to audited main with all six output files matching the tested proposal.

## Recommended change

Emit one local readiness hint after `_schedule_finalization_if_ready` successfully changes one row, commits, and closes its original session. The API reaper coalesces hints and performs its existing bounded full sweep sooner. Keep the ordinary timer for missed, stale, uninstrumented and cross-process work, and retain the existing short receiving cadence for live originals.

This changes two existing production owners: the collection-upload service and API reaper. It adds no dependency, package, persistent queue, scheduler, archive format, HTTP contract, polling-frequency increase or parallel capacity. Only a successful fresh readiness transition emits this hint; ordinary unit requests, finalization stage returns and Future completion do not.

The last sealed-payload receipt can first complete readiness facts. The subsequent successful conditional queue transition is the earliest confirmed queue-eligibility boundary available here without extra catalog reads. Notification carries no execution authority. Current due selection, open gate, exact claim registration, original receiving and independent child admission remain controlling.

## Evidence delivered

- **69 focused tests passed:** 28 new cases and 41 existing cases, including the existing readiness-scheduling growth witness's 238 samples.
- The actual reaper, with a one-day timer, woke after real completion/payload calls and admitted a real finalizer. A further hint preserved the original slot while its Future was done but its thread remained alive. The original thread was then joined and its exact result received.
- Notification added **zero SQL statements**: the measured point transition remains three statements with or without a listener.
- A burst of **1,024 hints caused one additional full sweep**, preserving all 15 existing steady-state owner calls per sweep.
- A finite old/new catalog-selection probe recorded post-commit-to-claim delays of 174–385 ms versus 3.3–3.5 ms with a deliberately small 400 ms fallback. This isolates the cadence mechanism; it does not time publication or establish a production speedup.
- Pinned focused Ruff/mypy, patch whitespace and applied-file identity checks passed.

The [design](DESIGN.md) specifies ownership, lifecycle and races. The [evidence](EVIDENCE.md) distinguishes the reported real-path timeline from the finite experiments. [Validation](VALIDATION.md) records actual commands, source identity and remaining work; [SOURCES.json](SOURCES.json) pins the inputs and proposed bytes.

## Source relationship and consumption

Reference-tree base: main `40cf854adc346421b161a03e7a79a8cb5e0e3632`. Requested timeline source: `8d278b6f7bc443a9919bcc5a6409a647cfd20f43`. Experiments and tests: inspected PR #1009 head `4d1bd1b7ead7c638e4a80137def7d406937098aa` plus the proposal. The affected existing production and reaper-test files are identical across those three sources; the full repositories are not identical. Product paths on this reference branch remain at the main base; proposed edits are carried only in `.reference/`.

Produced and published by OpenAI ChatGPT, GPT-6 Astra Pro, with three same-model source, race and evidence reviewers. Requested by the Riverhog integration agent and explicitly commissioned by the maintainer. User-facing date: 2026-11-27, America/Los_Angeles.

Apply/adapt through the normal integration rail; **do not merge this reference branch**. Record a finite Proffer disposition against its actual published SHA. Current-source PR qualification and one matched real publication diagnostic remain with integration. Record queue-commit/session-receiving, first eligible sweep, first admitted worker, complete publication and final receiving separately in that diagnostic.

This new slice does not supersede the recovery reference, answer the separate neutral-history request, authorize skipping completed-stage checks or change shared profile/budget authority. PR #1009 may finish its qualification independently. #1007 retains downstream whole-path/growth/recovery acceptance; no global hold is introduced.
