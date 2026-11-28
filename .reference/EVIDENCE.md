# Activation evidence and its limits

## The real-path account motivates the question

The integration owner's [timeline](https://github.com/nashspence/riverhog/issues/991#issuecomment-7233382688) records one intrusive real F1 at `8d278b6f7bc443a9919bcc5a6409a647cfd20f43`:

| Event after first client request | Reported time |
| --- | ---: |
| Completion reply | 41.671 s |
| Final payload PUT reply | 42.934 s |
| Existing 30 s maintenance sleep ends | 57.778 s |
| First observed physical worker begins | 57.805 s |

The account reports 14.845 s of post-PUT overlap with the existing sleep. The displayed rounded timestamps alone subtract to 14.844 s; preserve the reported overlap rather than presenting a new exact recomputation. The earlier completion reply cannot establish that the later payload had arrived.

Source review identifies the last sealed receipt and the subsequent conditional queue transition, but this trace does not timestamp their commit/session exit or record the UPDATE outcome. Thus 14.845 s is a concrete cadence candidate, not a directly measured eligibility-to-start interval or guaranteed saving. The next real diagnostic should add those precise owned boundaries.

The account's observed worker-body union omits other provider/crypto execution. Time outside that union is not demonstrated idle CPU. Client polling and maintenance waits overlap server work and cannot be added to it or subtracted as disjoint waste. The False-returning finalization-stage work is explicitly separate later review; no currentness or completed-stage check is removed here.

## Finite old/new queue-selection experiment

[idle_wake_probe.py](experiments/idle_wake_probe.py) compares the requested source's actual unmodified reaper with the proposal. Both use the real Core point-readiness transition and claim selector against small maintained catalog fixtures. The maintenance callback is deliberately narrowed to that selector so this experiment isolates activation cadence. It does not measure provider work, encryption, physical workers, full finalization or publication.

The fallback is **400 ms only for this finite experiment**. No production interval is changed. A timestamp is captured after the point transition's original transaction commits and its session closes, before callback delivery. The next exact claim is observed through the existing selector.

| Observation | Old reaper: commit to claim | Proposed reaper: commit to claim | Full sweeps through claim, each mode |
| --- | ---: | ---: | ---: |
| 1 | 383.785 ms | 3.407 ms | 2, including startup |
| 2 | 385.268 ms | 3.275 ms | 2, including startup |
| 3 | 173.935 ms | 3.517 ms | 2, including startup |

The old delay varies with where the durable transition falls within the existing timer period. The new hint allows an earlier sweep without increasing the number of sweeps through the first claim in these observations. These local values are neither per-file marginal costs nor a production timing contract, speedup ratio or expected saving from the real 30 s interval.

For precision, both modes use the proposed point-transition implementation. In the old-reaper mode no listener is bound; notification changes no query or authority there. The comparison isolates the old/new reaper, not two complete application versions. [Raw observations](evidence/idle-wake.json) record exact source/probe hashes, conditions and limitations; source stayed unchanged during the probe.

## Actual owned activation and original receiving

`test_archive_maintenance_activation.py` supplies a separate maintained proof with the actual maintenance function and collection-upload service. Its small valid fixture includes primary provenance and real completion/payload calls. Unrelated service interfaces use explicit idle fixtures; it is not a production environment.

The actual reaper first reaches idle with a **one-day fallback**. The subsequent real point transition emits one hint and the normal owner admits one first finalizer. The test holds that original thread alive after its Future has a result. A further hint forces another ordinary sweep, yet the same slot and original outcome remain retained. After reaper shutdown, the test releases and joins the original thread; normal archive maintenance then performs its actual zero-time join and receives the same Future. This proves first activation and receiving, not complete publication.

The helper does not bypass durable admission or inject an alternate executor. The only held-exit instrumentation is on the test's original physical thread. Existing independent tests additionally preserve terminal headroom, per-target admission, unrelated construction progress, retry cadence and claim acknowledgement handling.

## Work accounting

The maintained transition test observes the actual SQL statements: **three without a listener and the same three with a listener**. Notification adds no catalog statement. A duplicate point call after queuing does not notify again, and all three existing atomic-update races—changed terminal gate, payload readiness or planner—produce no new hint.

The actual full-maintenance test counts **1,024 notifications, two full sweeps including startup, and 30 steady-state owner-method calls**. This is one call to each of the existing 15 owners per sweep. Owner method behavior is stubbed in this count fixture; the actual activation test separately covers real collection work. The count establishes coalescing, not a claim that arbitrary full sweeps are cheap.

The existing complete readiness-scheduling growth witness passes **238 samples** against the changed source using the repository's unmodified numeric-evaluation resource profile. This is growth-evidence validation, separate from any production elapsed-time budget or shared profile admission.

## Reproduce

Use the repository-pinned environment in an integration checkout with the patch applied. Obtain the exact old API file outside that checkout:

```bash
activation_probe_dir=$(mktemp -d)
git show 8d278b6f7bc443a9919bcc5a6409a647cfd20f43:riverhog/src/riverhog_api/app.py > "$activation_probe_dir/baseline_app.py"
python /path/to/reference/.reference/experiments/idle_wake_probe.py \
  --repo "$PWD" \
  --baseline-app "$activation_probe_dir/baseline_app.py" \
  --output /path/to/new/idle-wake.json
```

`python` means the locked workspace interpreter. The output path must be new. This script is a finite reference experiment, not a new maintained product command or benchmark framework. The proposed test modules own the regressions. Current-source PR qualification and the matched real publication diagnostic remain integration's work.
