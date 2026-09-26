# #873 failed-run and missing-proof counterexamples

Source for all current cases: **`18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8`**. These are existing executable regression cases unless explicitly marked proposed. The audited CI unit suite passed, but its quiet output does not contain each child process's transcript. An expected failed child pytest run inside a passing regression is **not a failed CI workflow**, and a passing rejection test is **not a passed product qualification**.

## A. Successful attribution versus failed child runs

Existing node family: `tests/unit/test_operation_qualification.py::test_restart_attribution_requires_successful_test_outcome` ([source][tests]). Each generated child test records an event property before taking its outcome; the real observer plugin and child pytest exit status are inspected.

| Child outcome | Child pytest exit | Restart witness exported | Required conclusion |
| --- | --- | --- | --- |
| `pass` | 0 | Yes | Necessary positive control, not sufficient without correct identity, scope, and source. |
| `fail` | 1 | No | A property recorded before a failed assertion earns no pass; producer rejects the failed session. |
| `skip` | 0 | No | Successful session exit alone cannot replace an executed assertion. |
| `xfail` | 0 | No | Expected failure cannot qualify the claim. |
| `xpass` | 0 | No | Non-strict unexpected pass still carries `wasxfail` and is not accepted attribution. |
| `teardown_failure` | 1 | Yes | Call-phase success can leave a witness; the producer must reject the entire failed session. |

For nonzero child exits the enclosing regression calls `_load_operation_timings` and requires `QualificationError` matching `identity or test result`. The tests use a synthetic source SHA to isolate this rejection; those observations must never be offered as real source qualification.

## B. Scope and missing-witness failures

Existing node: `test_release_disposable_selection_satisfies_current_observation_requirements` in [test_operation_qualification.py][tests].

After a successful selected run, empty or partial restart witnesses produce `local_api_process_restart.status = not_established`. `None`, duplicate witnesses, an unrelated application, an unknown operation, and the wrong test node are rejected. Adding a fourth API event feed with matching cursor metadata leaves both the new row and the aggregate local subclaim unestablished until its own assertions run. This is the counterexample to inferring coverage from a shared helper or from yesterday's feed count.

Existing `test_timing_evidence_fails_closed_on_missing_local_operation` rejects empty observations against the nonempty required matrix. `test_exact_sha_evidence_contains_only_generated_current_rows` deletes a required client's wall-time evidence and expects rejection. Neither a complete timing matrix nor complete required-client timings alone establishes lifecycle semantics.

Existing `test_operation_timings_record_successful_responses` sends one successful response and one 404 to the same operation; the server/client sample counts remain one. This is a negative HTTP observation control, not a positive lifecycle test.

## C. Tests pass, but the named source must not qualify

Existing node families in [test_qualification_source.py][source-tests]:

- `test_observer_binds_the_executed_checkout_even_if_cleaned_before_report`: clean positive control and tracked, staged, untracked, during-test, and restored-before-report changes. Child tests pass in these development states. The dirty boundary is retained, and only the clean case can become accepted evidence after cleanup.
- `test_producer_rechecks_source_after_observation_and_report_construction`: tracked edits, a new untracked assertion file, different commit, or mutation during report construction are rejected. Relabeling old observations with a new HEAD also fails.
- `test_producer_rejects_unverified_or_mismatched_observation_source`: missing/empty source state, wrong HEAD, or `clean: 1` rather than literal `true` fails source matching.

The [source guard][source-guard] checks start, finish, and producer boundaries. It assumes exclusive checkout use and does not detect a transient edit restored entirely between checks. Ignored caches/generated files are excluded. It is source-state accounting, not cryptographic execution attestation or a concurrent-edit monitor.

## D. Honest report rejected; synthetic green is not evidence

Existing node: `test_release_operation_predicate_consumes_current_evidence_and_rejects_missing_proof` ([source][tests]). The current generated fixture with all four aggregate claims unestablished yields predicate exit 1 and stdout `false`. The test then overwrites those statuses to `passed` and checks the consumer's positive path; its comment explicitly labels this counterfactual. Returning any one required status to `not_established` or null again rejects it.

Do not copy that synthetic fixture, its all-`a` SHA, or fabricated timings into qualification evidence. Do not confuse successful execution of this regression with a successful real release predicate. Existing #866-related identity/schema mutations in the same file are not changed or re-owned by this reference.

## E. CLI-specific counterexample still needed for any new claim

**Proposed acceptance case, not an existing executed result in this reference:** use the intended real CLI invocation path with a fixture client that is reached, then causes an error or returns a nonzero terminal outcome after output begins. Require no positive projection witness even when callback entry or some JSON output occurred. Independently corrupt a human projection while leaving valid JSON, and require equivalence attribution to fail. Repeat with a failed enclosing test/teardown, and with a skipped pairing.

The existing paired collection-list/show assertions require both zero exits and expected outputs. FTP's successful fake-client rendering test does not compare equivalent human and machine semantics, so it is not a counterexample-resistant parity proof. Until explicit attribution and rejection controls exist for a proposed CLI claim, keep its aggregate status unestablished; do not restore the removed callback-count shortcut.

## Historical evidence is not current execution

The [#873 historical comment](https://github.com/nashspence/riverhog/issues/873#issuecomment-5692340898) records a clean run at `56e5f6309d75f219ce7531141158ef7a3d14c73b` with 154 selected tests and a rejected release predicate. The [#879 completion comment](https://github.com/nashspence/riverhog/issues/873#issuecomment-5695172551) records dirty-source refusals and a clean 156-test run at `515a346f2de3f1da5fca4fe95dd747a4a265b259`. These are explicitly historical owner attestations, not reproduced logs or current counts. The earlier zero-test selection failure is documented in #873/#876, but this reference has no exact failing-run transcript/SHA pair for it and does not invent one.

[tests]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_operation_qualification.py
[source-tests]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_qualification_source.py
[source-guard]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/scripts/qualification_source.py
