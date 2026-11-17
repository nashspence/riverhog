# Actual #998 reference evidence

## 1. Result and exact source

The final combined reference suite passed **65 cases**, with **0 failures, 0 errors and 0 skips**. Recorded pytest elapsed time was **4.090360889s**; the command took **6.104744337s** including process startup. The eight reference Python files were hashed before the run and confirmed unchanged during it. [Command/environment/hashes](evidence/reference_final_command.json) · [stdout](evidence/reference_final.txt) · [JUnit](evidence/reference_final.xml) · [repository timing](evidence/reference_final_timing.json).

Product source was `c28eceaa2dfec1b3712135469ba12230e74f5ffa` throughout. Source timing correctly records the added `.reference` working tree as unclean; it does not claim that the later published GitHub reference commit or a merge checkout ran production qualification. The publication process verifies that its reference Python bytes match this tested set and that all other product paths equal c28. [Machine-readable summary](VALIDATION.json).

Ruff lint and formatting passed **all eight** Python files. Strict mypy passed the typed proposed helpers `application_credit.py` and `ingress_lifetime.py`, with silent import following. Dynamic owner monkeypatch/counter harnesses were linted and executed, not presented as separately strict-typed product modules. [Static checks, timings and hashes](evidence/reference_static.json).

## 2. What the final reference cases establish

| Family | Cases | Actual scope |
| --- | ---: | --- |
| Application credit | 26 | Deterministic bounded cohort arithmetic: no sum of overlapping child times; in-flight first eligibility; prefix identity/regression; optional gates; unknown costs; old epochs; poll-time clipping; late cleanup overlapping another original. Authentication, durable grants and remote protocol are outside this helper. |
| Stove0 progress/lifetime | 13 | Actual target-support file writer and execution-session callbacks versus proposed volatile overlay; generation/stale/cancel guards; exact terminal reply loss; cold reopening; bounded active state; actual dispatcher and harmless local child receiving. |
| Ingress/lifetime | 19 | Actual FTP SQLite/manifest/storage fault windows, ASGI shutdown/thread ordering and Gogurt dispatch behavior; proposed retained worker/store composition and cold retry rotation. Active-claim fixtures do not implement new production accounting-pending states or capacity controls. |
| Relay/witness effects | 7 | Actual local stores with fixture external ports: delivery/cursor commit reply loss, exact witness result/nonce preservation, no-change SQL reduction and stale-generation refusal. |
| **Total** | **65** | Reference experiments and fault witnesses; not the complete proposed production implementation. |

The final run used the source's `scripts.ci_timing` and `qualification_numeric_policy`: max work `907920445364`, max integer bits `84`, reader SHA-256 `4f08bc6148007c7194b2451cdd885b04ada30806b6ee1f6c5e6d29433885720a`. These are actual execution settings, not support ceilings added to product contracts. Every test used task-owned temporary paths outside the checkout.

## 3. Separate unchanged-source baselines

These cohorts answer whether the reviewed current owners satisfy their existing focused tests. They do not prove the proposed contract changes. Retain their separate conditions and times rather than adding them to the final suite as one campaign.

| Cohort | Observed result | Recorded pytest elapsed | Evidence |
| --- | --- | ---: | --- |
| Core processing and SDK provenance/admission | 63 passed | 134.09s as rounded stdout; full timing retained | [command](evidence/core_baseline_command.json), [stdout](evidence/core_baseline.txt), [JUnit](evidence/core_baseline.xml), [timing](evidence/core_baseline_timing.json) |
| FTP/Gogurt/CLI | 18 passed | 72.54s as rounded stdout | [command](evidence/ingress_BASELINE_VALIDATION.json), [stdout](evidence/ingress_BASELINE.txt), [JUnit](evidence/ingress_BASELINE.xml) |
| Relay, witnesses and Review0/sampler identity | 15 passed | 3.666360135s | [command/results](evidence/effects_baseline.json), [JUnit](evidence/effects_baseline.xml), [timing](evidence/effects_baseline_timing.json) |
| Stove0 selected maintained cases | 9 passed, 1 failed initially; exact failed case then passed | 7.68s first cohort; 1.23s corrected case | [cohort](evidence/stove0_maintained_tests.xml), [exact rerun](evidence/stove0_effect_corrected.xml), [conditions/correction](evidence/stove0_validation.md) |

The Core command took **136.833119553s** including process startup. It ran all current collection-workflow and processing-provenance tests plus four original close/retry/completion-admission nodes, including parametrization. It exercised SQLite metadata, current claims/effect/no-output/parent settlement and real SDK Native logic through controlled API fixtures. It was not an installed end-to-end processing campaign.

The Stove0 corrected case originally failed before its synthetic effect because ambient SOCKS proxy variables caused the locked `httpx` client constructor to request optional `socksio`, which is not in the frozen environment. Only that no-network fixture was rerun with proxy variables omitted; no dependency or product source was changed. Ten distinct maintained Stove0 cases were observed passing after that correction. The final combined no-network reference subprocess also omitted these proxy variables; only variable names, not values, are recorded.

## 4. Causal counter observations

### Stove0 callback persistence

The same accepted/running original target request received **64 progress callbacks**, then the same successful terminal result. The actual current writer was compared with the reference volatile overlay.

| Measured boundary | Current | Proposed overlay |
| --- | ---: | ---: |
| Callback JSON file selections | 64 | 0 |
| Callback Python `os.fsync` calls | 128 | 0 |
| Callback encoded bytes | 23,927 | 0 |
| Callback-only elapsed, one ordered sample | 53.698308ms | 0.264129ms |
| Original acceptance + running file selections / sync calls | 2 / 4 | 2 / 4 |
| Terminal result file selections / sync calls | 1 / 2 | 1 / 2 |
| Four point status reads, writes / sync calls | 0 / 0 | 0 / 0 |

Exact final statuses matched. [Full measurement and source bindings](evidence/stove0_progress.json). These are Python writer/synchronization call counts and one local timing sample, not physical device writes, a power-loss test, a throughput guarantee or whole-application speedup. The result supports implementing the callback cut and measuring the complete maintained path next.

### Empty witness polling

For an exactly unchanged caught-up page, each actual Minisign/OpenTimestamps store entered one SQLite writer, upserted progress once and committed once. The proposed branch made **zero** such statements while checking a fresh generation/serial and returning the same accepted position. A concurrent generation change was rejected. Changed pages retain original atomic observation/intent/cursor acceptance. [Final rows](evidence/effects_final.json) · [initial dedicated comparison](evidence/effects_idle.json).

SQL traces do not reveal physical sync count or prove reduced total application latency. Both kinds of evidence are deliberately separate.

### Fault witnesses and their limits

FTP witnesses demonstrate current registration-reply-loss cleanup can leave an accepted claim row while deleting its original evidence tree. The proposed guard preserves that exact tree. Another witness shows current ASGI shutdown can close the API while the original thread still runs; the proposed slot retains the thread/result/storage until received. Current receipted-claim cleanup happens inside the PrimaryStore context; the proposed owner prevents reclamation before positive original close. These are reproducible source/fixture failures exercised as expected counterexamples, not unexplained failing test results.

Relay reply loss repeats the same accepted event identity where delivery is ambiguous; a committed cursor whose reply is lost is read back without resending its page. Witness commit-reply-loss injection preserves original accepted signatures/proof state on reopen and explicit view rebaseline. Calendar/signer/webhook ports are fixtures. No external effect was invoked.

Gogurt witnesses preserve the distinction between pre-entry failure and a nonzero/unknown entered command. The actual child used for receiving is harmless local Python. No installed native listener, arbitrary user script or platform service was dispatched.

## 5. Earlier runs and corrections remain visible

The initial Stove0 reference run had **11 passed, 2 failed** due to test-construction errors: treating an already frozen progress model as mutable, and expecting a new job identity after resealing the same fence. The corrected fixtures test independent copying and actually distinct fenced jobs. Its next run passed 13 cases, and the final combined run passed the same behavior. [Initial JUnit](evidence/stove0_tests_first.xml) and [narrative](evidence/stove0_validation.md) preserve the failures.

The raw initial Stove0 failure XML files retain six trailing-whitespace lines emitted in their original traceback text. An unrestricted `git diff --check` reports those lines; the authored source/documents and remaining evidence pass the scoped whitespace check. The raw reports are intentionally preserved rather than edited to conceal or cosmetically rewrite their failure output. XML parsing and all recorded test counts remain valid.

The first scoped credit static check exposed import ordering/long lines; those were fixed before the retained 25-case intermediate passing run. A later review added the old-original cleanup overlap case and conservative gate interval debit. The final 26 credit cases supersede that intermediate arithmetic evidence; [credit_tests.xml](evidence/credit_tests.xml) is historical intermediate evidence, not the final count. Ingress similarly expanded from 17 to 19 reference cases; its retained repository timing distinguishes those runs. No failed run is relabelled green.

A comment-only Stove0 correction after its standalone measurement is recorded in that measurement. The final combined run and static hashes bind the current comment-corrected files. Documentation/cross-component review continued after code verification; product paths and reference executable bytes did not change.

## 6. Environment and reproduction

The isolated `application-998-env` was created with the repository's frozen lock and all workspace packages/dev dependencies. Python **3.12.3**, pytest **9.1.1**, Ruff **0.16.0**, mypy **2.3.0**, SQLAlchemy **2.0.51**, SQLite **3.45.1**, Linux **6.18.57 x86_64/glibc2.41**. Lock SHA-256: `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`. Workspace editable packages resolve to this audited checkout; previous task environments were not repointed.

The final command and allowlisted environment are recorded verbatim in [reference_final_command.json](evidence/reference_final_command.json). To reproduce in another checkout, create its own frozen workspace environment, bind the source-owned qualification numeric profile, use that checkout plus `.reference/implementation` on `PYTHONPATH`, and choose fresh external `TMPDIR`, basetemp and timing directories. Then run:

```bash
python -m pytest -q -p scripts.ci_timing -o junit_family=xunit1 \
  --basetemp="$REFERENCE_998_TMP/pytest" \
  --junitxml="$REFERENCE_998_TMP/results.xml" \
  --durations=10 .reference/implementation
python -m ruff check .reference/implementation
python -m ruff format --check .reference/implementation
python -m mypy --follow-imports=silent --strict \
  .reference/implementation/application_credit.py \
  .reference/implementation/ingress_lifetime.py
```

`REFERENCE_998_TMP` denotes a newly selected external temporary directory, not a product setting. Set the numeric environment through `scripts.validation_numeric.bind_numeric_environment(..., qualification_numeric_policy())` before pytest, as in the recorded runners. Existing artifacts must not be overwritten to pretend a rerun occurred at the original source.

## 7. Not qualified here

The maintained command `make profile args='--scenario stove0-scale --output /workspace/scratch/939b84c043b0/998-processing-profile'` returned **2** at the missing-`mise` preflight (recipe **127**); `docker --version` also returned **127**. The profile, image preparation and workload never started. [Exact environment failure](evidence/effects_environment.json). This is blocked evidence, not a substitute passing profile.

No complete old/restart/adaptive processing run, production optional-overhead ratio, recovery benefit, final cleanup tail, installed Compose/image/codec/GPU, live calendar/signer/rclone/provider, PostgreSQL, macOS/Windows, physical crash/power-loss, release or production CI qualification is asserted. The new grant/phase transport, Core set conductor, retention quota cuts and Review0 protocol remain proposed integration work. The reference branch has no production PR/merge checkout; exact reference Actions availability is reported in its owning-issue publication record.

[QUALIFICATION.md](QUALIFICATION.md) supplies the concrete causal, growth, state, installed/backend/platform and final-source gates for that implementation. Those gates preserve current policies and public commitments; this local evidence does not waive them.
