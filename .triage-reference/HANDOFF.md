# Performance-objective triage handoff

Issue: #902
Convention: #903
Audited base: `e0413a288fe3e817db16d5e887b08622a24b3c94`

This branch is the repository-shaped form of the external reference bundle prepared for #902.
It is non-authoritative triage/integration input, not an accepted migration.

It carries the candidate performance-objective registry and human render, removes the nominal
1 Gbit/s adapter baseline in favor of explicit comparable measured references, centralizes
the existing database/listener numerical budgets, and includes the bundle's focused tests.

The original standalone bundle reported 90 passing standalone tests. Those results describe
the prepared bundle, not this GitHub branch after publication. Full checkout validation,
database qualification, compose smoke, contract-independence regression, lint/type checks,
and GitHub CI have not been rerun as part of publishing this reference branch.

An integration agent must revalidate against then-current `main` and the accepted #902
triage decision. Do not merge or mechanically apply this branch solely because it exists.
