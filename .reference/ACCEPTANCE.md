# Finite integration challenge matrix

#983 owns acceptance. This matrix makes its checks concrete; it does not add a release gate or claim these checks have run. R = demonstrated only by the isolated reference checks; I = required integration evidence, not performed by this producer.

| Case | Required observation | Reference / integration status |
| --- | --- | --- |
| Configuration domain | Absent/1 preserves nominal budgets; 4 multiplies once; 0.5 remains positive. Empty, malformed, zero, negative, NaN, infinities and unrepresentable factors fail clearly. | R for helper; I for direct pytest, Make and non-pytest startup. |
| Unit/backend conversion | Fractional seconds round upward for integer-unit consumers; no accidental zero-disable. Overflow/underflow and backend maximum failures are explicit, not silent clamping. | R for generic arithmetic/thread cap; I for real SQL/browser/shell/CI adapters. |
| One deadline | Remaining time decreases; retries do not reset or rescale it. Expiry reports nominal, factor and effective budget. Cleanup does not inherit only an exhausted budget. | R for arithmetic/diagnostic text; I for live observation and cleanup. |
| First slice | Seven thread waits become policy-controlled; same byte counts, callbacks, identities, workload and concurrency assertions. | R for patch applicability/syntax/AST equivalence after undoing plumbing; I for existing tests at 1 and 4. |
| Semantic stability | Unchanged injected-clock control-budget tests and configured timeout/lease values still pass; production configuration never reads the validation variable. | I. |
| Slow startup / actual timeout | Hold startup explicitly, establish readiness, then exercise actual expiration/cancellation and prove the native consumer stopped. | I; helper tests are not native containment proof. |
| Nonblocking and serialized work | Hold one activity, establish that a competing activity reached its critical phase, prove required independence/order and eventual outcomes. | I; immediate not-done and elapsed thresholds alone are inadequate. |
| Functional negative challenges | Deliberately serialize intended parallel work, block progress behind a callback, release occupied capacity early, or report cancellation before actual stop. The appropriate witness still detects each regression at 1 and 4. | I; use source-owned controlled regression fixtures, not production corruption. |
| Stalled test / error propagation | A deliberately never-completing test fixture produces nonzero/unestablished evidence and diagnosable cleanup, not pass/skip/xfail or an executor-exit hang. | I under test-owned supervision. |
| Subprocess and xdist | Factor plus nominal inputs arrive unchanged; effective 12 does not become 48 downstream. Mismatched/lost settings are observable. | R for a real Python child using the helper; I for maintained children/xdist. |
| Shell/container/native | The maintained container/native entrypoint receives the factor or explicitly effective budgets; workload formulas and related deadlines compose once. Required work and resource limits do not shrink. | I. |
| Browser and PostgreSQL | Nondefault factor affects real launch/context/navigation/action and SQL watchdogs; row/index/work assertions remain. Remove the separate SQL elapsed assertion rather than hiding it behind a larger SQL limit. | I. |
| Workflow/resource ceilings | A nondefault request reaches pre-job budget planning, outside-ci.yml jobs included; incompatible hard caps are explicit. No runtime env-only claim of extending a started job. | I with safe workflow fixtures and actual permitted CI. |
| Evidence integrity | Raw elapsed/rate/target data and work/source/run identity remain correct. Record effective conditions. Slow valid execution is not failed for missing a target; actual functional failures, incomplete required evidence and cleanup uncertainty remain unsuccessful. | I. |
| Full adoption | Reconcile current collected tests and maintained scripts/workflows against discovered candidates; narrow policy tests catch newly introduced unmanaged watchdogs without a second registry. | I. |

## Completion record

The integration handoff should identify exact delivered SHA, changes adopted or independently reproduced, normal local and exact-source GitHub gate results, exercised nondefault-scale families, unperformed platform checks, genuine failures and cleanup state. Keep progress/dispositions in #983. A reference test pass, a larger timeout, a lexical scan count or green tests only in the first slice is not repository-wide acceptance.
