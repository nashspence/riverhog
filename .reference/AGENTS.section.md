## Account for validation evidence

Checks record their results and timing as they run, without repeating work just to measure it. The [shared qualification definitions](scripts/ci_qualification.py) identify the work and its conditions without replacing contracts owned by individual operations.

Keep correctness, growth-contract conformance, and elapsed-time observations separate. Correctness failures, growth violations, and invalid required evidence fail validation. A valid timing miss does not.

Passing a test establishes only what its checks actually show. Tests can protect public behavior, internal rules, repository policy, or the tools used for validation—sometimes within the same test. Neither a test’s name nor where it runs determines what it proves. Evidence for a public promise must point to its owning contract and demonstrate the relevant behavior under the recorded conditions. Agreement between implementations proves only agreement unless that agreement is itself promised. Keep promises without sufficient proof visible, and preserve other required checks.

Report elapsed time even when no numerical target or comparable reference exists. Use credible performance evidence to recommend investigation or improvement. The stronger the evidence and the more serious its likely consequences, the stronger the recommendation may be.

Record significant findings and recommendations in GitHub. Performance targets do not create approval-to-proceed conditions or authorize weakening required tests, workloads, or safety limits.

In short, **evidence establishes only what its checks support; performance observations inform judgment.**
